package main

import (
	"errors"
	"net/http"
	"os"
	"strings"

	wopan "github.com/OpenListTeam/wopan-sdk-go"
)

type davWriteWarning struct {
	Kind    string `json:"kind"`
	Path    string `json:"path"`
	Method  string `json:"method,omitempty"`
	Message string `json:"message"`
	Reason  string `json:"reason"`
	Advice  string `json:"advice"`
}

func isDAVWrite(method string) bool {
	switch method {
	case "PUT", "MKCOL", "COPY", "MOVE", "DELETE", "PROPPATCH", "LOCK":
		return true
	}
	return false
}

// Only typed, explicit denials count as permission evidence. A timeout or
// credential/initialization failure alone does not prove a cloud ACL is read-only.
func cloudWriteDenied(err error) bool {
	var uncertain *davMutationError
	if errors.As(err, &uncertain) {
		return false
	}
	var fileError *os.PathError
	if errors.As(err, &fileError) {
		return false
	}
	if errors.Is(err, os.ErrPermission) {
		return true
	}
	var api *apiHTTPError
	var sdk *wopan.ResponseError
	var quark *quarkAPIError
	var local *davStatusError
	return errors.As(err, &api) && api.Code == http.StatusForbidden ||
		errors.As(err, &sdk) && sdk.HTTPStatus == http.StatusForbidden ||
		errors.As(err, &quark) && quark.HTTP == http.StatusForbidden ||
		errors.As(err, &local) && local.Code == http.StatusForbidden
}

func (m *davMount) warnWrite(path, method string, err error, redact func(string) string) {
	if err == nil {
		return
	}
	w := &davWriteWarning{Kind: "unverified", Path: path, Method: method, Reason: redact(err.Error()),
		Message: "写入通道未通过验证，上传可能失败；仍允许浏览和下载。",
		Advice:  "检查云盘写入授权、原生接口连接及挂载设置。修复后可重启 WebDAV 重新检查；读取权限有效的文件仍可下载。"}
	if cloudWriteDenied(err) {
		w.Kind = "denied"
		w.Message = "此路径的写入被拒绝，权限恢复前上传会失败；WebDAV 仍可用于浏览和下载。"
	}
	m.writeMu.Lock()
	m.writeWarning = w
	m.writeMu.Unlock()
}

func (m *davMount) writeWarningSnapshot() *davWriteWarning {
	m.writeMu.RLock()
	defer m.writeMu.RUnlock()
	if m.writeWarning == nil {
		return nil
	}
	w := *m.writeWarning
	return &w
}

func (m *davMount) clearWriteWarning(path, method string) {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	// Success on a different path/operation does not prove the denied ACL changed.
	if w := m.writeWarning; w != nil && w.Path == path && w.Method == method {
		m.writeWarning = nil
	}
}

func (s *davService) recordWriteResult(r *http.Request, state *davRequestState, status int) {
	if !isDAVWrite(r.Method) {
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/dav")
	if (r.Method == "COPY" || r.Method == "MOVE") && state.destination != "" {
		path = state.destination
	}
	path, err := davPath(path)
	if err != nil {
		return
	}
	for _, m := range s.fs.mounts {
		if path != "/"+m.Alias && !strings.HasPrefix(path, "/"+m.Alias+"/") {
			continue
		}
		if cloudWriteDenied(state.err) {
			m.warnWrite(path, r.Method, state.err, s.redact)
		} else if status >= 200 && status < 300 && status != http.StatusMultiStatus && state.err == nil {
			m.clearWriteWarning(path, r.Method)
		}
		return
	}
}
