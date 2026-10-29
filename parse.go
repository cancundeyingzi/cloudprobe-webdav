package main

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"
)

func decodeDocument(b []byte) (map[string]any, error) {
	b = bytes.TrimSpace(bytes.TrimPrefix(bytes.TrimSpace(b), []byte{0xef, 0xbb, 0xbf}))
	if len(b) == 0 {
		return nil, errors.New("响应为空")
	}
	if b[0] == '<' {
		return decodeXML(b)
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var m map[string]any
	if err := d.Decode(&m); err != nil {
		return nil, fmt.Errorf("响应不是有效 JSON/XML：%w", err)
	}
	if err := ensureEOF(d); err != nil {
		return nil, err
	}
	if m == nil {
		return nil, errors.New("响应不是 JSON 对象")
	}
	return m, nil
}
func decodeXML(b []byte) (map[string]any, error) {
	d := xml.NewDecoder(bytes.NewReader(b))
	var root xml.StartElement
	for {
		t, e := d.Token()
		if e != nil {
			return nil, fmt.Errorf("XML 根节点无效：%w", e)
		}
		if s, ok := t.(xml.StartElement); ok {
			root = s
			break
		}
	}
	v, e := readXMLNode(d, root, 0)
	if e != nil {
		return nil, e
	}
	for {
		t, e := d.Token()
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
		if _, ok := t.(xml.StartElement); ok {
			return nil, errors.New("XML 包含多个根节点")
		}
	}
	return map[string]any{root.Name.Local: v}, nil
}
func readXMLNode(d *xml.Decoder, start xml.StartElement, depth int) (any, error) {
	if depth > 64 {
		return nil, errors.New("XML 嵌套过深")
	}
	m := map[string]any{}
	for _, a := range start.Attr {
		m["@"+a.Name.Local] = a.Value
	}
	var text strings.Builder
	for {
		t, e := d.Token()
		if e != nil {
			return nil, e
		}
		switch v := t.(type) {
		case xml.StartElement:
			child, e := readXMLNode(d, v, depth+1)
			if e != nil {
				return nil, e
			}
			if old, ok := m[v.Name.Local]; ok {
				if list, yes := old.([]any); yes {
					m[v.Name.Local] = append(list, child)
				} else {
					m[v.Name.Local] = []any{old, child}
				}
			} else {
				m[v.Name.Local] = child
			}
		case xml.CharData:
			text.Write(v)
		case xml.EndElement:
			if v.Name == start.Name {
				if len(m) == 0 {
					return strings.TrimSpace(text.String()), nil
				}
				if s := strings.TrimSpace(text.String()); s != "" {
					m["#text"] = s
				}
				return m, nil
			}
		}
	}
}
func object(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return nil
}
func list(v any) []any {
	switch x := v.(type) {
	case []any:
		return x
	case map[string]any:
		return []any{x}
	default:
		return nil
	}
}
func checkCode(m map[string]any) error {
	for _, key := range []string{"code", "@resultCode", "resultCode"} {
		if v, ok := m[key]; ok {
			code := valueString(v)
			if code != "0" && code != "200" {
				msg := valueString(m["message"])
				if msg == "" {
					msg = valueString(m["msg"])
				}
				if code == "9999" || code == "10011" || code == "20011" {
					msg = "授权/会话失效，请更新对应令牌；" + msg
				}
				return &apiBusinessError{Code: code, Message: msg}
			}
		}
	}
	if meta := object(m["meta"]); meta != nil {
		return checkCode(meta)
	}
	return nil
}
func unwrap(m map[string]any) (map[string]any, error) {
	for i := 0; i < 8; i++ {
		if err := checkCode(m); err != nil {
			return nil, err
		}
		if data := object(m["data"]); data != nil {
			m = data
			continue
		}
		return m, nil
	}
	return nil, errors.New("响应封装层数过多")
}
func boolMarker(v any) (*bool, error) {
	var b bool
	switch strings.ToLower(valueString(v)) {
	case "true", "1":
		b = true
	case "false", "0":
		b = false
	default:
		return nil, errors.New("分页标记无效")
	}
	return &b, nil
}
func parsePage(provider string, b []byte) (Page, error) {
	doc, err := decodeDocument(b)
	if err != nil {
		return Page{}, stage("响应解析", err)
	}
	m, err := unwrap(doc)
	if err != nil {
		return Page{}, stage("接口业务状态", err)
	}
	p := Page{Items: []Item{}}
	if provider == "wopan" {
		raw, ok := m["fileInfoList"]
		if !ok || raw == nil {
			// 当前真实代理在请求未返回条目时会省略列表；只兼容明确成功且已结束分页的形态。
			meta := object(m["meta"])
			more, markerErr := boolMarker(m["nextPage"])
			knownShape := true
			for k := range m {
				if k != "meta" && k != "nextPage" && k != "fileInfoList" {
					knownShape = false
				}
			}
			if valueString(doc["code"]) == "0" && valueString(meta["code"]) == "200" && markerErr == nil && more != nil && !*more && knownShape {
				p.More = more
				p.ListOmitted = true
				p.Notes = []string{"接口成功且 nextPage=false，但省略了 fileInfoList。本页未返回文件条目；若整个查询均无条目，请核对目录 ID，不能据此认定整个网盘为空。"}
				return p, nil
			}
			return p, errors.New("目录响应缺少 fileInfoList，且未满足明确成功、结束分页的兼容条件；不能视为空目录")
		}
		if raw != nil && object(raw) == nil {
			if _, ok = raw.([]any); !ok {
				return p, errors.New("fileInfoList 类型错误")
			}
		}
		for _, entry := range list(raw) {
			x := object(entry)
			if x == nil {
				return p, errors.New("目录中有无效文件条目")
			}
			t := strings.ToLower(valueString(x["type"]))
			ft := strings.ToLower(valueString(x["fileType"]))
			it := Item{ID: valueString(x["id"]), FID: valueString(x["fid"]), Name: valueString(x["name"]), Size: number(x["fileSize"]), Directory: t == "0" || t == "folder" || t == "directory" || t == "dir" || ft == "folder" || ft == "directory" || (t == "" && ft == "0")}
			p.Items = append(p.Items, classify(provider, it))
		}
		if v, ok := m["nextPage"]; ok && v != nil {
			p.More, err = boolMarker(v)
		}
	} else {
		if result := object(m["result"]); result != nil {
			if err = checkCode(result); err != nil {
				return p, err
			}
			m = result
		}
		if d := object(m["getDiskResult"]); d != nil {
			m = d
		} else {
			return p, errors.New("移动目录响应缺少 getDiskResult；不能视为空目录")
		}
		v, ok := m["isCompleted"]
		if !ok {
			return p, errors.New("移动目录响应缺少 isCompleted，无法确认分页")
		}
		done, e := boolMarker(v)
		if e != nil {
			return p, e
		}
		more := !*done
		p.More = &more
		content := object(m["contentList"])
		if content == nil {
			if object(m["catalogList"]) != nil {
				content = map[string]any{"@length": "0"}
			} else {
				return p, errors.New("移动目录响应缺少 contentList")
			}
		}
		if raw, exists := content["contentInfo"]; exists && raw != nil {
			if object(raw) == nil {
				if _, ok := raw.([]any); !ok {
					return p, errors.New("contentInfo 类型错误，不能视为空列表")
				}
			}
		}
		for _, entry := range list(content["contentInfo"]) {
			x := object(entry)
			if x == nil {
				return p, errors.New("contentInfo 条目无效")
			}
			it := Item{ID: valueString(x["contentID"]), Name: valueString(x["contentName"]), Suffix: valueString(x["contentSuffix"]), Size: number(x["contentSize"])}
			if it.Suffix == "" {
				it.SampleParserDrop = "后缀为空"
			}
			p.Items = append(p.Items, classify(provider, it))
		}
		if _, ok := content["contentInfo"]; !ok && number(content["@length"]) != 0 {
			return p, errors.New("contentList 声称有文件但缺少 contentInfo")
		}
		if declared, exists := content["@length"]; exists && number(declared) != int64(len(p.Items)) {
			return p, errors.New("contentList.length 与实际条目数不符，无法确认列表完整性")
		}
		if catalogs := object(m["catalogList"]); catalogs != nil {
			var dirs []Item
			for _, v := range list(catalogs["catalogInfo"]) {
				x := object(v)
				if x == nil {
					return p, errors.New("目录条目无效")
				}
				dirs = append(dirs, classify(provider, Item{ID: valueString(x["catalogID"]), Name: valueString(x["catalogName"]), Directory: true}))
			}
			p.Items = append(dirs, p.Items...)
		}
	}
	return p, err
}
func parseDownload(provider, id string, b []byte) (string, error) {
	doc, e := decodeDocument(b)
	if e != nil {
		return "", e
	}
	m, e := unwrap(doc)
	if e != nil {
		return "", e
	}
	if provider == "wopan" {
		entries := list(m["downLoadInfos"])
		for _, v := range entries {
			x := object(v)
			if valueString(x["fid"]) == id {
				if u := valueString(x["downloadUrl"]); u != "" {
					return u, nil
				}
			}
		}
		return "", errors.New("下载响应未包含指定 fid 的地址")
	}
	if r := object(m["result"]); r != nil {
		m = r
	}
	if e = checkCode(m); e != nil {
		return "", e
	}
	if u := valueString(m["String"]); u != "" {
		return u, nil
	}
	return "", errors.New("移动下载响应缺少 result.String")
}
