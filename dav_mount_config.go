package main

import (
	"fmt"
	"strings"
)

// Validate the complete configuration, including accounts that are currently
// absent. Both interactive changes and startup recovery publish this form.
func validateSavedDAVMounts(mounts []davSavedMount) error {
	aliases := make(map[string]bool, len(mounts))
	accounts := make(map[string]davSavedMount, len(mounts))
	for _, m := range mounts {
		if safeFilename(m.Alias) != nil || aliases[m.Alias] {
			return fmt.Errorf("挂载名必须是唯一的普通文件夹名称：%s（包括暂未登录账号的保留挂载）", m.Alias)
		}
		aliases[m.Alias] = true
		if strings.TrimSpace(m.Identity) == "" {
			return fmt.Errorf("挂载 %s 的账号缺少稳定身份标识，请重新读取账号", m.Alias)
		}
		if strings.TrimSpace(m.Root) == "" || strings.ContainsAny(m.Root, "\r\n\x00") {
			return fmt.Errorf("挂载 %s 的根目录 ID 无效", m.Alias)
		}
		if m.UploadStorageMode != "" && m.UploadStorageMode != "memory" && m.UploadStorageMode != "disk" && m.UploadStorageMode != "auto" {
			return fmt.Errorf("挂载 %s 的上传缓存方式无效", m.Alias)
		}
		if previous, exists := accounts[m.Identity]; exists && (previous.Writable || m.Writable) {
			return fmt.Errorf("挂载 %s 与 %s 属于同账号；存在可写挂载时，该账号仅允许一个挂载。多个挂载必须全部只读，以免重叠目录绕过文件锁", previous.Alias, m.Alias)
		}
		accounts[m.Identity] = m
	}
	return nil
}
