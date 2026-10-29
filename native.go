package main

import (
	"bytes"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"
	"strings"
)

// 按指定 1.2.1 样本的 FieldRVA 定位生产环境密钥；不加载或执行 DLL。
func nativeKey(c NeteaseConfig) ([]byte, error) {
	if c.RawKeyBase64 != "" {
		b, e := base64.StdEncoding.DecodeString(c.RawKeyBase64)
		if e != nil || len(b) != 32 {
			return nil, errors.New("netease.raw_key_base64 必须编码 32 字节")
		}
		return b, nil
	}
	if c.NativeDLL == "" {
		return nil, errors.New("网易协议需要 netease.native_dll（该版本 ntkernel.dll 路径）或 raw_key_base64")
	}
	b, e := os.ReadFile(c.NativeDLL)
	if e != nil {
		return nil, e
	}
	h := sha256.Sum256(b)
	if !strings.EqualFold(hex.EncodeToString(h[:]), "D30191FB2E2C016BBBF6B41752ACB23598BFF6284DBB806F1AA6701E2F871CBA") {
		return nil, errors.New("ntkernel.dll 哈希不属于已分析的 1.2.1 样本，拒绝按旧偏移读取密钥")
	}
	const offset = 0xa2b710
	if len(b) < offset+32 {
		return nil, errors.New("DLL 长度不足")
	}
	key := append([]byte(nil), b[offset:offset+32]...)
	for i := range key {
		key[i] ^= 0x40
	}
	return key, nil
}
func uriEncode(s string) string { return strings.ReplaceAll(url.QueryEscape(s), "+", "%20") }

// 样本 nt.crypt.UrlEncode 将斜杠保留为未转义字符。
func nativeURLEncode(s string) string { return strings.ReplaceAll(uriEncode(s), "%2F", "/") }
func canonicalValue(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return valueString(v)
}
func queryParameters(params map[string]any) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		v := params[k]
		if v == nil {
			continue
		}
		switch x := v.(type) {
		case []any:
			if len(x) == 0 {
				continue
			}
		case map[string]any:
			if len(x) == 0 {
				continue
			}
		}
		if b.Len() > 0 {
			b.WriteByte('&')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(nativeURLEncode(canonicalValue(v)))
	}
	return b.String()
}
func nativeSign(method string, params map[string]any, body []byte, secTag, seed string, key []byte) string {
	var b strings.Builder
	if method == "GET" {
		if q := queryParameters(params); q != "" {
			b.WriteString(q)
			b.WriteByte('&')
		}
	}
	fmt.Fprintf(&b, "SecTag=%s&Seed=%s&Key=%s", secTag, seed, string(key))
	if method == "POST" && len(body) > 0 {
		b.WriteString("&post_content=")
		b.WriteString(nativeURLEncode(string(body)))
	}
	h := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(h[:])
}
func makeSecTag(c NeteaseConfig) string {
	if c.SecTag != "" {
		return c.SecTag
	}
	h := md5.Sum([]byte(c.DeviceID))
	return strings.ToUpper(hex.EncodeToString(h[:]))
}
func encryptNative(key, plain []byte) ([]byte, error) {
	if len(key) < 16 {
		return nil, errors.New("原生协议密钥长度不足")
	}
	// 样本 aligned_data 对大于 16 字节的输入只保留前 16 字节。
	block, e := aes.NewCipher(key[:16])
	if e != nil {
		return nil, e
	}
	g, e := cipher.NewGCMWithNonceSize(block, 16)
	if e != nil {
		return nil, e
	}
	var compressed bytes.Buffer
	z := gzip.NewWriter(&compressed)
	if _, e = z.Write(plain); e != nil {
		return nil, e
	}
	if e = z.Close(); e != nil {
		return nil, e
	}
	nonce := make([]byte, 16)
	if _, e = rand.Read(nonce); e != nil {
		return nil, e
	}
	frame := append([]byte{16}, nonce...)
	return g.Seal(frame, nonce, compressed.Bytes(), nil), nil
}
func decryptNative(key, frame []byte, limit int64) ([]byte, error) {
	if len(frame) < 2 {
		return nil, errors.New("加密帧太短")
	}
	n := int(frame[0])
	if n < 1 || n > 32 || len(frame) < 1+n+16 {
		return nil, errors.New("加密帧 IV/长度无效")
	}
	if len(key) < 16 {
		return nil, errors.New("原生协议密钥长度不足")
	}
	block, e := aes.NewCipher(key[:16])
	if e != nil {
		return nil, e
	}
	g, e := cipher.NewGCMWithNonceSize(block, n)
	if e != nil {
		return nil, e
	}
	data, e := g.Open(nil, frame[1:1+n], frame[1+n:], nil)
	if e != nil {
		return nil, errors.New("AES-GCM 校验失败，协议或密钥不匹配")
	}
	z, e := gzip.NewReader(bytes.NewReader(data))
	if e != nil {
		return nil, fmt.Errorf("解密后的 gzip 无效：%w", e)
	}
	defer z.Close()
	return readBounded(z, limit)
}
func readBounded(r io.Reader, limit int64) ([]byte, error) {
	b, e := io.ReadAll(io.LimitReader(r, limit+1))
	if e != nil {
		return nil, e
	}
	if int64(len(b)) > limit {
		return nil, fmt.Errorf("响应超过上限 %d 字节", limit)
	}
	return b, nil
}
