package main

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

func readQuarkOSSCompletion(response *http.Response, reader io.Reader) error {
	const limit = 1 << 20
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return fmt.Errorf("读取夸克合并响应中断，结果待核验：%w", err)
	}
	if len(body) > limit {
		return errors.New("夸克合并响应超过 1 MiB，结果待核验")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var failure struct {
			Code string `xml:"Code"`
		}
		if xml.Unmarshal(body, &failure) == nil && failure.Code == "NoSuchUpload" {
			return &quarkPartError{HTTP: &upstreamHTTPError{code: response.StatusCode, source: "夸克 OSS 完成请求"}, Code: failure.Code, Detail: "（NoSuchUpload）"}
		}
		return &upstreamHTTPError{code: response.StatusCode, after: retryAfterHeader(response.Header), source: "夸克 OSS 完成请求"}
	}
	body = bytes.TrimSpace(bytes.TrimPrefix(body, []byte{0xef, 0xbb, 0xbf}))
	if len(body) == 0 {
		return errors.New("夸克合并响应为空，结果待核验")
	}
	if body[0] == '{' {
		doc, err := quarkJSON(body)
		if err != nil {
			return err
		}
		accepted := false
		// Quark's native OSS callback uses the explicit legacy verdict
		// {"Status":"OK"}, rather than the account API's lower-case envelope.
		if raw, exists := doc["Status"]; exists {
			switch strings.ToUpper(valueString(raw)) {
			case "OK", "SUCCESS", "200", "0":
				accepted = true
			default:
				return errors.New("夸克 OSS 回调 Status 未确认成功")
			}
		}
		for _, key := range []string{"error", "error_info", "error_message"} {
			if message := valueString(doc[key]); message != "" && message != "0" {
				return errors.New("夸克合并回调包含错误，需核验上传任务")
			}
		}
		for _, key := range []string{"code", "status"} {
			if raw, exists := doc[key]; exists {
				value := valueString(raw)
				if value != "0" && value != "200" {
					return fmt.Errorf("夸克合并回调 %s=%s，未视为成功", key, value)
				}
				accepted = true
			}
		}
		if raw, exists := doc["success"]; exists {
			success, ok := raw.(bool)
			if !ok || !success {
				return errors.New("夸克合并回调未确认成功")
			}
			accepted = true
		}
		if !accepted {
			return errors.New("夸克合并回调缺少成功标识，结果待核验")
		}
		return nil
	}
	decoder := xml.NewDecoder(bytes.NewReader(body))
	var result struct {
		XMLName xml.Name
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	if err := decoder.Decode(&result); err != nil {
		return fmt.Errorf("夸克合并 XML 无效，结果待核验：%w", err)
	}
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		switch value := token.(type) {
		case xml.CharData:
			if strings.TrimSpace(string(value)) != "" {
				return errors.New("夸克合并 XML 含未知尾部数据")
			}
		case xml.Comment, xml.ProcInst:
		default:
			return errors.New("夸克合并 XML 含额外文档")
		}
	}
	if result.XMLName.Local == "Error" {
		return errors.New("夸克 OSS 合并返回错误，需核验上传任务")
	}
	if result.XMLName.Local != "CompleteMultipartUploadResult" {
		return errors.New("夸克合并 XML 类型未知，结果待核验")
	}
	return nil
}
