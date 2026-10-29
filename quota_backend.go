package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	wopan "github.com/OpenListTeam/wopan-sdk-go"
)

func parseWopanQuota(raw []byte) (davQuota, error) {
	m, err := decodeDocument(raw)
	if err != nil {
		return davQuota{}, err
	}
	// QueryCloudUsageInfo has its own success code inside the already-validated
	// dispatcher envelope. It is "0000", unlike the proxy's generic 0/200 codes.
	if valueString(m["code"]) == "0000" {
		m["code"] = "0"
	}
	if m, err = unwrap(m); err != nil {
		return davQuota{}, err
	}
	info := object(m["usageInfo"])
	total, te := nonnegativeInteger(info["byteTotalSize"])
	used, ue := nonnegativeInteger(info["byteUsedSize"])
	if te != nil || ue != nil {
		return davQuota{}, errors.New("联通容量响应缺少有效 byteTotalSize/byteUsedSize；未用展示字符串或零值代替")
	}
	return davQuota{Total: total, Used: used}, nil
}

func requestWopanQuota(ctx context.Context, w *wopan.WoClient) (davQuota, error) {
	if w.Phone == "" {
		user, err := w.AppQueryUser(woContext(ctx))
		if err != nil {
			return davQuota{}, fmt.Errorf("读取联通容量所需账号标识：%w", err)
		}
		if user.UserId == "" {
			return davQuota{}, errors.New("联通容量查询缺少账号标识")
		}
		w.Phone = user.UserId
	}
	var raw json.RawMessage
	_, err := w.RequestWoHome(wopan.KeyQueryCloudUsageInfo, wopan.Json{"phoneNum": w.Phone, "clientId": wopan.DefaultClientID}, wopan.JsonSecret, &raw, woContext(ctx))
	if err != nil {
		return davQuota{}, fmt.Errorf("QueryCloudUsageInfo：%w", err)
	}
	return parseWopanQuota(raw)
}

func (d *wopanDAVDrive) Quota(ctx context.Context) (q davQuota, err error) {
	if err = d.directMu.LockContext(ctx); err != nil {
		return q, err
	}
	defer d.directMu.Unlock()
	attempt := func(w *wopan.WoClient, source string) (davQuota, error) {
		finish := logPhase(ctx, "读取联通账号容量", source, "QueryCloudUsageInfo")
		q, e := requestWopanQuota(ctx, w)
		finish(e)
		return q, e
	}
	q, err = attempt(d.w, d.source())
	if err == nil || ctx.Err() != nil {
		return q, err
	}
	if d.usingWeb {
		// A separate client preserves the selected upload authorization. Read-only
		// queries can safely try the bound token after the website route fails.
		logFallback(ctx, "联通网页容量接口", "联通绑定授权容量接口", err)
		d.mu.Lock()
		token := d.client.cfg.Wopan.AccessToken
		d.mu.Unlock()
		w := wopan.DefaultWithAccessToken(token)
		w.SetHttpClient(providerMetadataClient(10 * time.Second))
		w.OnRefreshToken(func(string, string) {})
		q, err = attempt(w, "联通绑定授权直连")
		if err == nil || ctx.Err() != nil {
			return q, err
		}
	}
	if wopanAuthError(err) {
		if re := d.refreshNative(ctx); re == nil {
			if !d.usingWeb {
				return attempt(d.w, "联通绑定授权直连（刷新后）")
			}
			w := wopan.DefaultWithAccessToken(d.client.cfg.Wopan.AccessToken)
			w.SetHttpClient(providerMetadataClient(10 * time.Second))
			w.OnRefreshToken(func(string, string) {})
			return attempt(w, "联通绑定授权直连（刷新后）")
		}
	}
	return q, err
}

func parseMobileQuota(raw []byte, method string) (davQuota, error) {
	m, err := mobileResult(raw)
	if err != nil {
		return davQuota{}, err
	}
	var total, used int64
	var te, ue error
	if method == "getDiskInfo" {
		info := object(m["diskInfo"])
		total, te = nonnegativeInteger(info["diskSize"])
		var free int64
		free, ue = nonnegativeInteger(info["freeDiskSize"])
		if te == nil && ue == nil && free > total {
			return davQuota{}, errors.New("移动容量响应的可用空间大于总空间")
		}
		used = total - free
	} else if method == "getServiceDiskInfoV2" {
		var info map[string]any
		for _, v := range list(m["serviceDiskInfo"]) {
			x := object(v)
			if valueString(x["service"]) == "1" {
				if info != nil {
					return davQuota{}, errors.New("移动容量响应包含重复的个人云业务")
				}
				info = x
			}
		}
		total, te = nonnegativeInteger(info["totalSize"])
		used, ue = nonnegativeInteger(info["usedSize"])
	} else {
		return davQuota{}, errors.ErrUnsupported
	}
	// The official miniapp DiskInfo/ServiceDiskInfo APIs return MB. As in the
	// OpenList 139 driver, convert using 1024*1024, with checked arithmetic.
	if te != nil || ue != nil || total > math.MaxInt64/(1<<20) || used > math.MaxInt64/(1<<20) {
		return davQuota{}, errors.New("移动容量响应字段缺失、不是非负整数或换算字节会溢出")
	}
	return davQuota{Total: total << 20, Used: used << 20}, nil
}

func (d *mobileDAVDrive) Quota(ctx context.Context) (davQuota, error) {
	if err := d.mu.LockContext(ctx); err != nil {
		return davQuota{}, err
	}
	defer d.mu.Unlock()
	var last error
	for _, method := range []string{"getDiskInfo", "getServiceDiskInfoV2"} {
		for _, direct := range []bool{false, true} {
			if ctx.Err() != nil {
				return davQuota{}, ctx.Err()
			}
			cl, source := d.client, "网易代理"
			if direct {
				var err error
				cl, err = d.directClient()
				if err != nil {
					last = err
					continue
				}
				source = "移动官方接口"
			}
			if last != nil {
				logFallback(ctx, "上一容量方案", source+" "+method, last)
			}
			finish := logPhase(ctx, "读取移动账号容量", source, method)
			body := `<?xml version="1.0" encoding="UTF-8"?><` + method + `/>`
			raw, err := cl.mobile(ctx, "richlifeApp/devapp/"+method, body)
			var q davQuota
			if err == nil {
				q, err = parseMobileQuota(raw, method)
			}
			if mobileAuthError(err) && ctx.Err() == nil {
				if re := d.refresh(ctx); re == nil {
					raw, err = cl.mobile(ctx, "richlifeApp/devapp/"+method, body)
					if err == nil {
						q, err = parseMobileQuota(raw, method)
					}
				}
			}
			finish(err)
			if err == nil {
				return q, nil
			}
			last = err
		}
	}
	return davQuota{}, fmt.Errorf("移动容量查询的代理、官方直连及备用方法均未成功：%w", last)
}
