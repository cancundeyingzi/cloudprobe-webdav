package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type guiInstance struct {
	lease *os.File
	file  string
	once  sync.Once
}

func claimGUIInstance(ctx context.Context, data string) (*guiInstance, string, error) {
	lease, err := tryLockFile(filepath.Join(data, "gui-instance.lock"))
	file := filepath.Join(data, "gui-instance.bin")
	if err == nil {
		return &guiInstance{lease: lease, file: file}, "", nil
	}
	lockErr := err
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for {
		if address, err := existingGUIInstance(ctx, file); err == nil {
			return nil, address, nil
		}
		if err := davWait(ctx, 50*time.Millisecond); err != nil {
			return nil, "", fmt.Errorf("无法取得管理界面实例锁或连接现有实例，请等待原进程启动完成后重试：%w", lockErr)
		}
	}
}

func validateGUIAddress(address string) (*url.URL, error) {
	u, err := url.Parse(address)
	if err != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" || u.User != nil || u.Path != "/" || u.RawQuery != "" || len(u.Fragment) != 40 {
		return nil, errors.New("管理界面地址记录无效")
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 {
		return nil, errors.New("管理界面端口无效")
	}
	if _, err := hex.DecodeString(u.Fragment); err != nil {
		return nil, errors.New("管理界面会话无效")
	}
	return u, nil
}

func existingGUIInstance(ctx context.Context, file string) (string, error) {
	raw, err := os.Open(file)
	if err != nil {
		return "", err
	}
	defer raw.Close()
	protected, err := readBounded(raw, 16<<10)
	if err != nil {
		return "", err
	}
	plain, err := unprotectLocal(protected)
	if err != nil {
		return "", err
	}
	address := string(plain)
	u, err := validateGUIAddress(address)
	if err != nil {
		return "", err
	}
	endpoint := "http://" + u.Host + "/api/status"
	request, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("X-CloudProbe-UI", u.Fragment)
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 300 * time.Millisecond, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", errors.New("现有管理界面未接受会话")
	}
	body, err := readBounded(response.Body, 1<<20)
	var status struct {
		Version string `json:"version"`
	}
	if err != nil || json.Unmarshal(body, &status) != nil || status.Version == "" {
		return "", errors.New("现有进程未返回有效的管理状态")
	}
	return address, nil
}

func (instance *guiInstance) publish(address string) error {
	if _, err := validateGUIAddress(address); err != nil {
		return err
	}
	data, err := protectLocal([]byte(address))
	if err != nil {
		return err
	}
	return writePrivateBytes(instance.file, data)
}

func (instance *guiInstance) Close() {
	instance.once.Do(func() {
		_ = os.Remove(instance.file)
		_ = instance.lease.Close()
	})
}
