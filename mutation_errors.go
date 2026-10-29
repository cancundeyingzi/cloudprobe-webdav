package main

import (
	"errors"
	"fmt"

	wopan "github.com/OpenListTeam/wopan-sdk-go"
)

type apiBusinessError struct{ Code, Message string }

func (e *apiBusinessError) Error() string { return fmt.Sprintf("业务码 %s，%s", e.Code, e.Message) }

type apiHTTPError struct {
	Code int
	Err  error
}

func (e *apiHTTPError) Error() string { return e.Err.Error() }
func (e *apiHTTPError) Unwrap() error { return e.Err }

type davMutationError struct {
	Phase   string
	Partial bool
	Err     error
}

func (e *davMutationError) Error() string {
	if e.Partial {
		return fmt.Sprintf("操作已部分执行，%s 阶段失败，需要核验实际状态：%v", e.Phase, e.Err)
	}
	return fmt.Sprintf("%s 写入结果未确认：%v", e.Phase, e.Err)
}
func (e *davMutationError) Unwrap() error { return e.Err }

func businessErrorCode(err error) string {
	var business *apiBusinessError
	if errors.As(err, &business) {
		return business.Code
	}
	var sdk *wopan.ResponseError
	if errors.As(err, &sdk) {
		return sdk.RspCode
	}
	return ""
}
func confirmedMutationRejection(err error) bool {
	var uncertain *davMutationError
	if errors.As(err, &uncertain) {
		return false
	}
	var api *apiHTTPError
	if errors.As(err, &api) && (api.Code == 401 || api.Code == 403 || api.Code == 501) {
		return true
	}
	var sdk *wopan.ResponseError
	if errors.As(err, &sdk) {
		if sdk.HTTPStatus == 401 || sdk.HTTPStatus == 403 || sdk.HTTPStatus == 501 {
			return true
		}
		switch sdk.RspCode {
		case "500020001", "1001", "9999", "10011", "10012":
			return true
		}
	}
	var business *apiBusinessError
	if errors.As(err, &business) {
		switch business.Code {
		case "9999", "10011", "10012", "20011":
			return true
		}
	}
	return false
}
func mutationFailure(err error, phase string, alreadyChanged bool) error {
	if err == nil {
		return nil
	}
	if !alreadyChanged && confirmedMutationRejection(err) {
		return &davNotExecuted{err}
	}
	return &davMutationError{Phase: phase, Partial: alreadyChanged, Err: err}
}
