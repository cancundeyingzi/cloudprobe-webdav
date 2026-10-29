package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"
)

type Item struct {
	ID                string `json:"id"`
	FID               string `json:"fid,omitempty"`
	Name              string `json:"name"`
	Suffix            string `json:"suffix"`
	Kind              string `json:"kind,omitempty"`
	Size              int64  `json:"size,omitempty"`
	Directory         bool   `json:"directory,omitempty"`
	SampleParserDrop  string `json:"sample_parser_drop,omitempty"`
	SubtitleCandidate bool   `json:"baomihua_subtitle_candidate"`
	FilterReason      string `json:"filter_reason"`
}
type Expected struct {
	Name       string `json:"name"`
	Returned   bool   `json:"returned"`
	Conclusion string `json:"conclusion"`
}
type Report struct {
	Version    string     `json:"tool_version"`
	Time       string     `json:"time"`
	Mode       string     `json:"mode"`
	Provider   string     `json:"provider"`
	Directory  string     `json:"directory,omitempty"`
	Complete   bool       `json:"query_completed"`
	StopReason string     `json:"stop_reason"`
	Pages      int        `json:"pages"`
	Items      []Item     `json:"items"`
	Expected   []Expected `json:"expected_files,omitempty"`
	Error      string     `json:"error,omitempty"`
	Notes      []string   `json:"notes"`
}
type Page struct {
	Items       []Item
	More        *bool
	Notes       []string
	ListOmitted bool
}
type StageError struct {
	Stage   string
	Message string
}

func (e *StageError) Error() string { return e.Stage + "：" + e.Message }
func stage(s string, e error) error {
	if e == nil {
		return nil
	}
	return &StageError{s, e.Error()}
}
func newReport(provider, mode, dir string) Report {
	return Report{Version: version, Time: time.Now().Format(time.RFC3339), Provider: provider, Mode: mode, Directory: dir, Items: []Item{}, Notes: []string{"显示原始列表中被工具识别的条目；字幕候选仅模拟后缀筛选，不含同名匹配和服务端智能匹配。", "未返回不等于无权限或文件不存在；判断必须结合分页、错误和目录范围。", "本工具不会在列目录时下载文件正文。"}}
}
func classify(provider string, it Item) Item {
	it.Suffix = strings.ToLower(strings.TrimPrefix(it.Suffix, "."))
	if it.Suffix == "" {
		it.Suffix = strings.ToLower(strings.TrimPrefix(filepath.Ext(it.Name), "."))
	}
	it.Kind = fileKind(it)
	it.SubtitleCandidate = false
	switch {
	case it.Directory:
		it.FilterReason = "文件夹，不进入字幕候选"
	case it.SampleParserDrop != "":
		it.FilterReason = "样本解析层会忽略：" + it.SampleParserDrop
	case it.ID == "" && provider == "mobile":
		it.FilterReason = "样本解析层会忽略：缺少文件 ID"
	case it.Name == "":
		it.FilterReason = "样本解析层会忽略：缺少文件名"
	case it.Suffix == "" && provider == "mobile":
		it.FilterReason = "样本解析层会忽略：后缀为空"
	case provider == "wopan" && it.FID == "":
		it.FilterReason = "样本字幕路径会忽略：fid 为空"
	default:
		allowed := "|ass|srt|ssa|"
		if provider == "wopan" {
			allowed = "|ass|srt|ssa|vtt|sub|idx|sup|"
		}
		if strings.Contains(allowed, "|"+it.Suffix+"|") {
			it.SubtitleCandidate = true
			it.FilterReason = "通过字幕后缀白名单；后续仍可能做匹配筛选"
		} else {
			it.FilterReason = "接口已返回，字幕后缀筛选会忽略"
		}
	}
	return it
}

func fileKind(it Item) string {
	if it.Directory {
		return "文件夹"
	}
	lower := strings.ToLower(it.Name)
	for _, ext := range []string{".zip", ".7z", ".rar", ".tar", ".gz", ".bz2", ".xz", ".zst"} {
		if len(lower) > 4 && lower[len(lower)-4] == '.' {
			tail := lower[len(lower)-3:]
			if tail[0] >= '0' && tail[0] <= '9' && tail[1] >= '0' && tail[1] <= '9' && tail[2] >= '0' && tail[2] <= '9' && strings.HasSuffix(lower[:len(lower)-4], ext) {
				return "压缩包（分卷）"
			}
		}
	}
	switch it.Suffix {
	case "zip", "7z", "rar", "tar", "gz", "bz2", "xz", "zst":
		return "压缩包"
	case "mkv", "mp4", "avi", "mov", "wmv", "flv", "ts", "m2ts", "webm", "m4v":
		return "视频"
	case "ass", "srt", "ssa", "vtt", "sub", "idx", "sup":
		return "字幕"
	case "txt", "md", "nfo", "json", "xml", "log", "yaml", "yml":
		return "文本"
	default:
		if it.Suffix != "" {
			return it.Suffix
		}
		return "文件"
	}
}

func defaultDirectory(provider string) string {
	if provider == "wopan" || provider == "quark" {
		return "0"
	}
	return "root"
}
func applyExpected(r *Report, names []string) {
	for _, name := range names {
		if name == "" {
			continue
		}
		e := Expected{Name: name}
		for _, it := range r.Items {
			if strings.EqualFold(name, it.Name) {
				e.Returned = true
				break
			}
		}
		if e.Returned {
			e.Conclusion = "本次目录响应已返回其文件信息；不代表下载过正文"
		} else if !r.Complete {
			e.Conclusion = "未在已读取页中找到，查询未完成，不能判断"
		} else {
			e.Conclusion = "本次目录查询未返回；不能单凭此项区分权限、服务端筛选、目录或文件名差异"
		}
		r.Expected = append(r.Expected, e)
	}
}
func fingerprint(items []Item) string {
	b, _ := json.Marshal(items)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
func ensureEOF(d *json.Decoder) error {
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return errors.New("JSON 后存在多余内容")
	}
	return nil
}
func valueString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return string(x)
	case nil:
		return ""
	default:
		b, _ := compactJSON(v)
		return string(b)
	}
}
func number(v any) int64 { var n int64; _, _ = fmt.Sscan(valueString(v), &n); return n }
func compactJSON(v any) ([]byte, error) {
	var b strings.Builder
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(v); err != nil {
		return nil, err
	}
	return []byte(strings.TrimSuffix(b.String(), "\n")), nil
}
