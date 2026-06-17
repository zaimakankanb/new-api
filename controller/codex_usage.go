package controller

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/relay/channel/codex"
	"github.com/QuantumNous/new-api/service"

	"github.com/gin-gonic/gin"
)

func GetCodexChannelUsage(c *gin.Context) {
	channelId, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		common.ApiError(c, fmt.Errorf("invalid channel id: %w", err))
		return
	}

	ch, err := model.GetChannelById(channelId, true)
	if err != nil {
		common.ApiError(c, err)
		return
	}
	if ch == nil {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "channel not found"})
		return
	}
	if ch.Type != constant.ChannelTypeCodex {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "channel type is not Codex"})
		return
	}
	if ch.ChannelInfo.IsMultiKey {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "multi-key channel is not supported"})
		return
	}

	oauthKey, err := codex.ParseOAuthKey(strings.TrimSpace(ch.Key))
	if err != nil {
		common.SysError("failed to parse oauth key: " + err.Error())
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "解析凭证失败，请检查渠道配置"})
		return
	}
	accessToken := strings.TrimSpace(oauthKey.AccessToken)
	accountID := strings.TrimSpace(oauthKey.AccountID)
	if accessToken == "" {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "codex channel: access_token is required"})
		return
	}
	if accountID == "" {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "codex channel: account_id is required"})
		return
	}

	client, err := service.NewProxyHttpClient(ch.GetSetting().Proxy)
	if err != nil {
		common.ApiError(c, err)
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
	defer cancel()

	statusCode, body, err := service.FetchCodexWhamUsage(ctx, client, ch.GetBaseURL(), accessToken, accountID)
	if err != nil {
		common.SysError("failed to fetch codex usage: " + err.Error())
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "获取用量信息失败，请稍后重试"})
		return
	}

	if (statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden) && strings.TrimSpace(oauthKey.RefreshToken) != "" {
		refreshCtx, refreshCancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
		defer refreshCancel()

		res, refreshErr := service.RefreshCodexOAuthTokenWithProxy(refreshCtx, oauthKey.RefreshToken, ch.GetSetting().Proxy)
		if refreshErr == nil {
			oauthKey.AccessToken = res.AccessToken
			oauthKey.RefreshToken = res.RefreshToken
			oauthKey.LastRefresh = time.Now().Format(time.RFC3339)
			oauthKey.Expired = res.ExpiresAt.Format(time.RFC3339)
			if strings.TrimSpace(oauthKey.Type) == "" {
				oauthKey.Type = "codex"
			}

			encoded, encErr := common.Marshal(oauthKey)
			if encErr == nil {
				_ = model.DB.Model(&model.Channel{}).Where("id = ?", ch.Id).Update("key", string(encoded)).Error
				model.InitChannelCache()
				service.ResetProxyClientCache()
			}

			ctx2, cancel2 := context.WithTimeout(c.Request.Context(), 15*time.Second)
			defer cancel2()
			statusCode, body, err = service.FetchCodexWhamUsage(ctx2, client, ch.GetBaseURL(), oauthKey.AccessToken, accountID)
			if err != nil {
				common.SysError("failed to fetch codex usage after refresh: " + err.Error())
				c.JSON(http.StatusOK, gin.H{"success": false, "message": "获取用量信息失败，请稍后重试"})
				return
			}
		}
	}

	var payload any
	if common.Unmarshal(body, &payload) != nil {
		payload = string(body)
	}

	ok := statusCode >= 200 && statusCode < 300
	resp := gin.H{
		"success":         ok,
		"message":         "",
		"upstream_status": statusCode,
		"data":            payload,
	}
	if !ok {
		resp["message"] = fmt.Sprintf("upstream status: %d", statusCode)
	}
	c.JSON(http.StatusOK, resp)
}

// GetCodexChannelConsumerResets 手动获取并持久化 consumer resets 信息
func GetCodexChannelConsumerResets(c *gin.Context) {
	channelId, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		common.ApiError(c, fmt.Errorf("invalid channel id: %w", err))
		return
	}

	ch, err := model.GetChannelById(channelId, true)
	if err != nil {
		common.ApiError(c, err)
		return
	}
	if ch == nil {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "channel not found"})
		return
	}
	if ch.Type != constant.ChannelTypeCodex {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "channel type is not Codex"})
		return
	}
	if ch.ChannelInfo.IsMultiKey {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "multi-key channel is not supported"})
		return
	}

	oauthKey, err := codex.ParseOAuthKey(strings.TrimSpace(ch.Key))
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "解析凭证失败，请检查渠道配置"})
		return
	}
	accessToken := strings.TrimSpace(oauthKey.AccessToken)
	accountID := strings.TrimSpace(oauthKey.AccountID)
	if accessToken == "" || accountID == "" {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "codex channel: access_token and account_id are required"})
		return
	}

	client, err := service.NewProxyHttpClient(ch.GetSetting().Proxy)
	if err != nil {
		common.ApiError(c, err)
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
	defer cancel()

	statusCode, body, err := service.FetchCodexWhamUsage(ctx, client, ch.GetBaseURL(), accessToken, accountID)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "获取用量信息失败: " + err.Error()})
		return
	}

	// 处理 401/403 —— 尝试刷新凭证后重试
	if (statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden) && strings.TrimSpace(oauthKey.RefreshToken) != "" {
		refreshCtx, refreshCancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
		defer refreshCancel()

		res, refreshErr := service.RefreshCodexOAuthTokenWithProxy(refreshCtx, oauthKey.RefreshToken, ch.GetSetting().Proxy)
		if refreshErr == nil {
			oauthKey.AccessToken = res.AccessToken
			oauthKey.RefreshToken = res.RefreshToken
			oauthKey.LastRefresh = time.Now().Format(time.RFC3339)
			oauthKey.Expired = res.ExpiresAt.Format(time.RFC3339)
			if strings.TrimSpace(oauthKey.Type) == "" {
				oauthKey.Type = "codex"
			}

			encoded, encErr := common.Marshal(oauthKey)
			if encErr == nil {
				_ = model.DB.Model(&model.Channel{}).Where("id = ?", ch.Id).Update("key", string(encoded)).Error
				model.InitChannelCache()
				service.ResetProxyClientCache()
			}

			ctx2, cancel2 := context.WithTimeout(c.Request.Context(), 15*time.Second)
			defer cancel2()
			statusCode, body, err = service.FetchCodexWhamUsage(ctx2, client, ch.GetBaseURL(), oauthKey.AccessToken, accountID)
			if err != nil {
				c.JSON(http.StatusOK, gin.H{"success": false, "message": "获取用量信息失败: " + err.Error()})
				return
			}
		}
	}

	if statusCode < 200 || statusCode >= 300 {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": fmt.Sprintf("upstream status: %d", statusCode)})
		return
	}

	// 解析并持久化 consumer resets
	usage, parseErr := service.ParseCodexWhamUsage(body)
	if parseErr != nil {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "解析用量数据失败: " + parseErr.Error()})
		return
	}

	otherSettings := ch.GetOtherSettings()
	if usage.ConsumerResets != nil {
		if usage.ConsumerResets.Primary != nil {
			otherSettings.CodexConsumerResets5h = usage.ConsumerResets.Primary.Remaining
		}
		if usage.ConsumerResets.Secondary != nil {
			otherSettings.CodexConsumerResets7d = usage.ConsumerResets.Secondary.Remaining
		}
	}
	otherSettings.CodexConsumerResetsSyncedAt = time.Now().Unix()

	settingsBytes, _ := common.Marshal(otherSettings)
	_ = model.DB.Model(&model.Channel{}).Where("id = ?", ch.Id).Update("other_settings", string(settingsBytes)).Error

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data": gin.H{
			"consumer_resets_5h":  otherSettings.CodexConsumerResets5h,
			"consumer_resets_7d":  otherSettings.CodexConsumerResets7d,
			"synced_at":          otherSettings.CodexConsumerResetsSyncedAt,
			"auto_reset_enabled": otherSettings.CodexAutoResetEnabled,
			"auto_reset_5h":     otherSettings.CodexAutoReset5h,
			"auto_reset_7d":     otherSettings.CodexAutoReset7d,
			"auto_reset_threshold": otherSettings.CodexAutoResetThreshold,
		},
	})
}

// ConsumeCodexChannelReset 手动消耗一次 consumer reset
func ConsumeCodexChannelReset(c *gin.Context) {
	channelId, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		common.ApiError(c, fmt.Errorf("invalid channel id: %w", err))
		return
	}

	var req struct {
		Window string `json:"window" binding:"required"` // "5h" or "7d"
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "message": "参数错误: window 必须是 '5h' 或 '7d'"})
		return
	}

	var windowParam string
	switch req.Window {
	case "5h":
		windowParam = "primary"
	case "7d":
		windowParam = "secondary"
	default:
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "message": "window 必须是 '5h' 或 '7d'"})
		return
	}

	ch, err := model.GetChannelById(channelId, true)
	if err != nil {
		common.ApiError(c, err)
		return
	}
	if ch == nil {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "channel not found"})
		return
	}
	if ch.Type != constant.ChannelTypeCodex {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "channel type is not Codex"})
		return
	}
	if ch.ChannelInfo.IsMultiKey {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "multi-key channel is not supported"})
		return
	}

	oauthKey, err := codex.ParseOAuthKey(strings.TrimSpace(ch.Key))
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "解析凭证失败"})
		return
	}
	accessToken := strings.TrimSpace(oauthKey.AccessToken)
	accountID := strings.TrimSpace(oauthKey.AccountID)
	if accessToken == "" || accountID == "" {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "codex channel: access_token and account_id are required"})
		return
	}

	client, err := service.NewProxyHttpClient(ch.GetSetting().Proxy)
	if err != nil {
		common.ApiError(c, err)
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
	defer cancel()

	statusCode, respBody, err := service.ConsumeCodexReset(ctx, client, ch.GetBaseURL(), accessToken, accountID, windowParam)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "消耗重置失败: " + err.Error()})
		return
	}

	ok := statusCode >= 200 && statusCode < 300

	// 消耗成功后更新本地计数
	if ok {
		otherSettings := ch.GetOtherSettings()
		switch req.Window {
		case "5h":
			if otherSettings.CodexConsumerResets5h > 0 {
				otherSettings.CodexConsumerResets5h--
			}
		case "7d":
			if otherSettings.CodexConsumerResets7d > 0 {
				otherSettings.CodexConsumerResets7d--
			}
		}
		otherSettings.CodexConsumerResetsSyncedAt = time.Now().Unix()
		settingsBytes, _ := common.Marshal(otherSettings)
		_ = model.DB.Model(&model.Channel{}).Where("id = ?", ch.Id).Update("other_settings", string(settingsBytes)).Error
	}

	var payload any
	if common.Unmarshal(respBody, &payload) != nil {
		payload = string(respBody)
	}

	resp := gin.H{
		"success":         ok,
		"upstream_status": statusCode,
		"data":            payload,
	}
	if !ok {
		resp["message"] = fmt.Sprintf("upstream status: %d", statusCode)
	} else {
		resp["message"] = fmt.Sprintf("成功消耗 %s 窗口的一次重置", req.Window)
	}
	c.JSON(http.StatusOK, resp)
}
