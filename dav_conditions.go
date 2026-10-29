package main

import (
	"errors"
	"net/http"
	"os"
	"strings"
	"time"
)

// A conditional mutation cannot use a recently cached directory snapshot.
// Reads requesting HTTP validators also need fresh metadata before ServeContent.
func (f *davFS) refreshPath(name string) {
	clean, err := davPath(name)
	if err != nil {
		return
	}
	alias := strings.Split(strings.TrimPrefix(clean, "/"), "/")[0]
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	f.cacheVersion++
	for key := range f.cache {
		if strings.HasPrefix(key, alias+"\x00") {
			delete(f.cache, key)
		}
	}
}

type davTagList struct {
	any  bool
	tags []string
}

func parseDAVTags(value string) (davTagList, error) {
	var out davTagList
	value = strings.TrimSpace(value)
	if value == "*" {
		out.any = true
		return out, nil
	}
	for value != "" {
		start := value
		if strings.HasPrefix(value, "W/") {
			value = value[2:]
		}
		if len(value) == 0 || value[0] != '"' {
			return out, errors.New("无效 ETag 条件")
		}
		value = value[1:]
		end := strings.IndexByte(value, '"')
		if end < 0 {
			return out, errors.New("无效 ETag 条件")
		}
		for _, c := range []byte(value[:end]) {
			if c < 0x21 || c == 0x7f {
				return out, errors.New("无效 ETag 条件")
			}
		}
		value = value[end+1:]
		out.tags = append(out.tags, start[:len(start)-len(value)])
		value = strings.TrimSpace(value)
		if value == "" {
			break
		}
		if value[0] != ',' {
			return out, errors.New("无效 ETag 条件")
		}
		value = strings.TrimSpace(value[1:])
		if value == "" {
			return out, errors.New("无效 ETag 条件")
		}
	}
	if len(out.tags) == 0 {
		return out, errors.New("无效 ETag 条件")
	}
	return out, nil
}

func (s *davService) preconditions(w http.ResponseWriter, r *http.Request, target string) bool {
	match := strings.Join(r.Header.Values("If-Match"), ",")
	none := strings.Join(r.Header.Values("If-None-Match"), ",")
	unmodified := r.Header.Get("If-Unmodified-Since")
	if match == "" && none == "" && unmodified == "" {
		return true
	}
	var matches, nones davTagList
	var err error
	if match != "" {
		matches, err = parseDAVTags(match)
	}
	if err == nil && none != "" {
		nones, err = parseDAVTags(none)
	}
	if err != nil {
		http.Error(w, err.Error(), 400)
		return false
	}
	fi, err := s.fs.Stat(r.Context(), target)
	if err != nil && !os.IsNotExist(err) {
		http.Error(w, s.safeError(err), 502)
		return false
	}
	exists := err == nil
	etag := ""
	if exists {
		etag, _ = fi.(davInfo).ETag(r.Context())
	}
	contains := func(list davTagList, strong bool) bool {
		if !exists {
			return false
		}
		if list.any {
			return true
		}
		for _, tag := range list.tags {
			if strong {
				if !strings.HasPrefix(etag, "W/") && !strings.HasPrefix(tag, "W/") && tag == etag {
					return true
				}
			} else if strings.TrimPrefix(tag, "W/") == strings.TrimPrefix(etag, "W/") {
				return true
			}
		}
		return false
	}
	failed := match != "" && !contains(matches, true) || none != "" && contains(nones, false)
	if match == "" && exists && unmodified != "" {
		if at, err := http.ParseTime(unmodified); err == nil && fi.ModTime().Truncate(time.Second).After(at) {
			failed = true
		}
	}
	if failed {
		http.Error(w, "文件已变化或同名文件已存在，条件检查失败", 412)
		return false
	}
	return true
}
