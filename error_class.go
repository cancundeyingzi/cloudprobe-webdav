package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	wopan "github.com/OpenListTeam/wopan-sdk-go"
)

type bridgeResponseError struct {
	Code    int
	Message string
}

func (e *bridgeResponseError) Error() string { return e.Message }

func transferHTTPStatus(err error) int {
	var upstream *upstreamHTTPError
	var api *apiHTTPError
	var sdk *wopan.ResponseError
	var quark *quarkAPIError
	switch {
	case errors.As(err, &upstream):
		return upstream.code
	case errors.As(err, &api):
		return api.Code
	case errors.As(err, &sdk):
		return sdk.HTTPStatus
	case errors.As(err, &quark):
		return quark.HTTP
	}
	return 0
}

func wopanAuthError(err error) bool {
	if transferHTTPStatus(err) == 401 {
		return true
	}
	code, message := businessErrorCode(err), ""
	var sdk *wopan.ResponseError
	var api *apiBusinessError
	var bridge *bridgeResponseError
	switch {
	case errors.As(err, &sdk):
		message = sdk.Message
	case errors.As(err, &api):
		message = api.Message
	case errors.As(err, &bridge):
		code = fmt.Sprint(bridge.Code)
	}
	switch code {
	case "1001", "9999", "10011", "20011":
		return true
	}
	message = strings.ToLower(message)
	return strings.Contains(message, "无效登录") || strings.Contains(message, "授权/会话失效") || strings.Contains(message, "token expired")
}

func retryableTransfer(err error) bool {
	var denied *persistentForbiddenError
	if errors.As(err, &denied) {
		return false
	}
	if wopanAuthError(err) {
		return true
	}
	if status := transferHTTPStatus(err); status != 0 {
		return retryableHTTP(status)
	}
	var quark *quarkAPIError
	if errors.As(err, &quark) {
		return quark.Auth
	}
	var network net.Error
	return errors.As(err, &network) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

type persistentForbiddenError struct{ Err error }

func (e *persistentForbiddenError) Error() string {
	return fmt.Sprintf("同一传输通道连续 5 次被拒绝（HTTP 403），请求已停止；请检查云盘权限或重新登录后重试：%v", e.Err)
}
func (e *persistentForbiddenError) Unwrap() error { return e.Err }

type forbiddenRetries struct {
	mu       sync.Mutex
	count    int
	terminal error
}

func (r *forbiddenRetries) failure(err error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.terminal != nil {
		return r.terminal
	}
	if transferHTTPStatus(err) != 403 {
		r.count = 0
		return err
	}
	r.count++
	if r.count >= 5 {
		r.terminal = &persistentForbiddenError{Err: err}
		return r.terminal
	}
	return err
}

func (r *forbiddenRetries) stopped() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.terminal
}
