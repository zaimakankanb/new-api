package service

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"

	"github.com/bytedance/gopkg/util/gopool"
)

const (
	codexAutoResetTickInterval    = 5 * time.Minute
	codexAutoResetBatchSize       = 200
	codexAutoResetTimeout         = 15 * time.Second
	codexAutoResetDefaultThreshold = 80.0
)

var (
	codexAutoResetOnce    sync.Once
	codexAutoResetRunning atomic.Bool
)

// StartCodexAutoResetTask 启动 Codex 自动重置后台任务
// 每 5 分钟扫描所有启用的 Codex 渠道，同步 consumer resets 计数，
// 并在满足条件时自动消耗重置
func StartCodexAutoResetTask() {
	codexAutoResetOnce.Do(func() {
		if !common.IsMasterNode {
			return
		}

		gopool.Go(func() {
			logger.LogInfo(context.Background(), fmt.Sprintf(
				"codex auto-reset task started: tick=%s default_threshold=%.0f%%",
				codexAutoResetTickInterval, codexAutoResetDefaultThreshold,
			))

			ticker := time.NewTicker(codexAutoResetTickInterval)
			defer ticker.Stop()

			runCodexAutoResetOnce()
			for range ticker.C {
				runCodexAutoResetOnce()
			}
		})
	})
}

func runCodexAutoResetOnce() {
	if !codexAutoResetRunning.CompareAndSwap(false, true) {
		return
	}
	defer codexAutoResetRunning.Store(false)

	ctx := context.Background()
	now := time.Now()

	var synced, resetConsumed, scanned int

	offset := 0
	for {
		var channels []*model.Channel
		err := model.DB.
			Select("id", "name", "key", "status", "other_settings", "channel_info", "setting").
			Where("type = ? AND status = ?",
				constant.ChannelTypeCodex,
				common.ChannelStatusEnabled,
			).
			Order("id asc").
			Limit(codexAutoResetBatchSize).
			Offset(offset).
			Find(&channels).Error
		if err != nil {
			logger.LogError(ctx, fmt.Sprintf("codex auto-reset: query channels failed: %v", err))
			return
		}
		if len(channels) == 0 {
			break
		}
		offset += codexAutoResetBatchSize

		for _, ch := range channels {
			if ch == nil {
				continue
			}
			scanned++

			// 跳过 multi-key 渠道
			if ch.ChannelInfo.IsMultiKey {
				continue
			}

			rawKey := strings.TrimSpace(ch.Key)
			if rawKey == "" {
				continue
			}

			oauthKey, err := parseCodexOAuthKey(rawKey)
			if err != nil {
				continue
			}

			accessToken := strings.TrimSpace(oauthKey.AccessToken)
			accountID := strings.TrimSpace(oauthKey.AccountID)
			if accessToken == "" || accountID == "" {
				continue
			}

			// 创建带代理的 HTTP 客户端
			client, err := NewProxyHttpClient(ch.GetSetting().Proxy)
			if err != nil {
				logger.LogWarn(ctx, fmt.Sprintf("codex auto-reset: channel_id=%d create http client failed: %v", ch.Id, err))
				continue
			}

			// 1. 获取 wham/usage 数据
			fetchCtx, cancel := context.WithTimeout(ctx, codexAutoResetTimeout)
			statusCode, body, err := FetchCodexWhamUsage(fetchCtx, client, ch.GetBaseURL(), accessToken, accountID)
			cancel()

			if err != nil {
				logger.LogWarn(ctx, fmt.Sprintf("codex auto-reset: channel_id=%d fetch usage failed: %v", ch.Id, err))
				continue
			}

			// 处理 401/403 —— 尝试刷新凭证后重试
			if statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden {
				refreshToken := strings.TrimSpace(oauthKey.RefreshToken)
				if refreshToken == "" {
					continue
				}
				refreshCtx, refreshCancel := context.WithTimeout(ctx, codexAutoResetTimeout)
				newKey, _, refreshErr := RefreshCodexChannelCredential(refreshCtx, ch.Id, CodexCredentialRefreshOptions{ResetCaches: false})
				refreshCancel()
				if refreshErr != nil {
					logger.LogWarn(ctx, fmt.Sprintf("codex auto-reset: channel_id=%d refresh credential failed: %v", ch.Id, refreshErr))
					continue
				}
				accessToken = strings.TrimSpace(newKey.AccessToken)
				if accessToken == "" {
					continue
				}

				retryCtx, retryCancel := context.WithTimeout(ctx, codexAutoResetTimeout)
				statusCode, body, err = FetchCodexWhamUsage(retryCtx, client, ch.GetBaseURL(), accessToken, accountID)
				retryCancel()
				if err != nil {
					logger.LogWarn(ctx, fmt.Sprintf("codex auto-reset: channel_id=%d retry fetch usage failed: %v", ch.Id, err))
					continue
				}
			}

			if statusCode < 200 || statusCode >= 300 {
				if common.DebugEnabled {
					logger.LogDebug(ctx, "codex auto-reset: channel_id=%d upstream status %d, skip", ch.Id, statusCode)
				}
				continue
			}

			// 2. 解析响应
			usage, err := ParseCodexWhamUsage(body)
			if err != nil {
				logger.LogWarn(ctx, fmt.Sprintf("codex auto-reset: channel_id=%d parse usage failed: %v", ch.Id, err))
				continue
			}

			// 3. 更新 OtherSettings 中的 consumer_resets 计数
			otherSettings := ch.GetOtherSettings()
			settingsChanged := false

			if usage.ConsumerResets != nil {
				if usage.ConsumerResets.Primary != nil {
					if otherSettings.CodexConsumerResets5h != usage.ConsumerResets.Primary.Remaining {
						otherSettings.CodexConsumerResets5h = usage.ConsumerResets.Primary.Remaining
						settingsChanged = true
					}
				}
				if usage.ConsumerResets.Secondary != nil {
					if otherSettings.CodexConsumerResets7d != usage.ConsumerResets.Secondary.Remaining {
						otherSettings.CodexConsumerResets7d = usage.ConsumerResets.Secondary.Remaining
						settingsChanged = true
					}
				}
			}
			otherSettings.CodexConsumerResetsSyncedAt = now.Unix()
			settingsChanged = true
			synced++

			// 4. 检查是否需要自动消耗重置
			if otherSettings.CodexAutoResetEnabled && usage.RateLimit != nil {
				threshold := otherSettings.CodexAutoResetThreshold
				if threshold <= 0 {
					threshold = codexAutoResetDefaultThreshold
				}

				// 检查 5h 窗口
				if otherSettings.CodexAutoReset5h &&
					usage.RateLimit.PrimaryWindow != nil &&
					usage.RateLimit.PrimaryWindow.UsedPercent >= threshold &&
					usage.ConsumerResets != nil &&
					usage.ConsumerResets.Primary != nil &&
					usage.ConsumerResets.Primary.Remaining > 0 {

					consumeCtx, consumeCancel := context.WithTimeout(ctx, codexAutoResetTimeout)
					consumeStatus, _, consumeErr := ConsumeCodexReset(consumeCtx, client, ch.GetBaseURL(), accessToken, accountID, "primary")
					consumeCancel()

					if consumeErr != nil {
						logger.LogWarn(ctx, fmt.Sprintf(
							"codex auto-reset: channel_id=%d consume 5h reset failed: %v",
							ch.Id, consumeErr,
						))
					} else if consumeStatus >= 200 && consumeStatus < 300 {
						otherSettings.CodexConsumerResets5h--
						resetConsumed++
						logger.LogInfo(ctx, fmt.Sprintf(
							"codex auto-reset: channel_id=%d name=%s consumed 5h reset (usage=%.1f%% threshold=%.0f%%), remaining=%d",
							ch.Id, ch.Name, usage.RateLimit.PrimaryWindow.UsedPercent, threshold, otherSettings.CodexConsumerResets5h,
						))
					} else {
						logger.LogWarn(ctx, fmt.Sprintf(
							"codex auto-reset: channel_id=%d consume 5h reset upstream status %d",
							ch.Id, consumeStatus,
						))
					}
				}

				// 检查 7d 窗口
				if otherSettings.CodexAutoReset7d &&
					usage.RateLimit.SecondaryWindow != nil &&
					usage.RateLimit.SecondaryWindow.UsedPercent >= threshold &&
					usage.ConsumerResets != nil &&
					usage.ConsumerResets.Secondary != nil &&
					usage.ConsumerResets.Secondary.Remaining > 0 {

					consumeCtx, consumeCancel := context.WithTimeout(ctx, codexAutoResetTimeout)
					consumeStatus, _, consumeErr := ConsumeCodexReset(consumeCtx, client, ch.GetBaseURL(), accessToken, accountID, "secondary")
					consumeCancel()

					if consumeErr != nil {
						logger.LogWarn(ctx, fmt.Sprintf(
							"codex auto-reset: channel_id=%d consume 7d reset failed: %v",
							ch.Id, consumeErr,
						))
					} else if consumeStatus >= 200 && consumeStatus < 300 {
						otherSettings.CodexConsumerResets7d--
						resetConsumed++
						logger.LogInfo(ctx, fmt.Sprintf(
							"codex auto-reset: channel_id=%d name=%s consumed 7d reset (usage=%.1f%% threshold=%.0f%%), remaining=%d",
							ch.Id, ch.Name, usage.RateLimit.SecondaryWindow.UsedPercent, threshold, otherSettings.CodexConsumerResets7d,
						))
					} else {
						logger.LogWarn(ctx, fmt.Sprintf(
							"codex auto-reset: channel_id=%d consume 7d reset upstream status %d",
							ch.Id, consumeStatus,
						))
					}
				}
			}

			// 5. 保存更新后的 OtherSettings
			if settingsChanged {
				settingsBytes, marshalErr := common.Marshal(otherSettings)
				if marshalErr == nil {
					_ = model.DB.Model(&model.Channel{}).Where("id = ?", ch.Id).
						Update("other_settings", string(settingsBytes)).Error
				}
			}
		}
	}

	if common.DebugEnabled {
		logger.LogDebug(ctx, "codex auto-reset: scanned=%d synced=%d reset_consumed=%d", scanned, synced, resetConsumed)
	}
}
