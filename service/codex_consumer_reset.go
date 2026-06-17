package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/QuantumNous/new-api/common"
)

// ---------- wham/usage 响应结构（仅提取需要的字段） ----------

type CodexRateLimitWindow struct {
	UsedPercent        float64 `json:"used_percent"`
	ResetAt            int64   `json:"reset_at"`
	ResetAfterSeconds  int64   `json:"reset_after_seconds"`
	LimitWindowSeconds int64   `json:"limit_window_seconds"`
}

type CodexRateLimit struct {
	PlanType        string                `json:"plan_type"`
	Allowed         bool                  `json:"allowed"`
	LimitReached    bool                  `json:"limit_reached"`
	PrimaryWindow   *CodexRateLimitWindow `json:"primary_window"`
	SecondaryWindow *CodexRateLimitWindow `json:"secondary_window"`
}

type CodexConsumerResetWindow struct {
	Remaining int   `json:"remaining"`
	ExpiresAt int64 `json:"expires_at,omitempty"`
}

type CodexConsumerResets struct {
	Primary   *CodexConsumerResetWindow `json:"primary"`
	Secondary *CodexConsumerResetWindow `json:"secondary"`
}

type CodexWhamUsageResponse struct {
	RateLimit      *CodexRateLimit      `json:"rate_limit"`
	ConsumerResets *CodexConsumerResets  `json:"consumer_resets"`
	PlanType       string               `json:"plan_type"`
	UserID         string               `json:"user_id"`
	Email          string               `json:"email"`
	AccountID      string               `json:"account_id"`
}

// ParseCodexWhamUsage 将 FetchCodexWhamUsage 返回的 raw body 解析为结构体
func ParseCodexWhamUsage(body []byte) (*CodexWhamUsageResponse, error) {
	var resp CodexWhamUsageResponse
	if err := common.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parse wham/usage response: %w", err)
	}
	return &resp, nil
}

// ConsumeCodexReset 调用 POST /backend-api/wham/consumer_resets 消耗一次重置
//
// window 参数: "primary"（5h 窗口）或 "secondary"（7d 窗口）
//
// 注意：此 API 的确切请求格式未公开文档，以下实现基于逆向分析推断，
// 如果上游返回 4xx，可能需要调整请求体格式。
func ConsumeCodexReset(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	accessToken string,
	accountID string,
	window string,
) (statusCode int, body []byte, err error) {
	if client == nil {
		return 0, nil, fmt.Errorf("nil http client")
	}
	bu := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if bu == "" {
		return 0, nil, fmt.Errorf("empty baseURL")
	}
	at := strings.TrimSpace(accessToken)
	aid := strings.TrimSpace(accountID)
	if at == "" {
		return 0, nil, fmt.Errorf("empty accessToken")
	}
	if aid == "" {
		return 0, nil, fmt.Errorf("empty accountID")
	}
	if window != "primary" && window != "secondary" {
		return 0, nil, fmt.Errorf("invalid window: %s (must be 'primary' or 'secondary')", window)
	}

	// 构造请求体
	reqBody := map[string]string{"reset_window": window}
	jsonBody, err := common.Marshal(reqBody)
	if err != nil {
		return 0, nil, fmt.Errorf("marshal request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, bu+"/backend-api/wham/consumer_resets", bytes.NewReader(jsonBody))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+at)
	req.Header.Set("chatgpt-account-id", aid)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if req.Header.Get("originator") == "" {
		req.Header.Set("originator", "codex_cli_rs")
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}
