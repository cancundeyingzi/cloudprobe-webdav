package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"
)

func runFolder(base string) (string, error) {
	if e := os.MkdirAll(base, 0700); e != nil {
		return "", e
	}
	return os.MkdirTemp(base, "run-"+time.Now().Format("20060102-150405")+"-")
}
func displayName(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return '�'
		}
		return r
	}, s)
}
func printReport(w io.Writer, r Report) {
	fmt.Fprintf(w, "模式：%s　云盘：%s　页数：%d　条目：%d\n查询完成：%t　结束原因：%s\n", r.Mode, r.Provider, r.Pages, len(r.Items), r.Complete, r.StopReason)
	t := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(t, "文件名\tID\t字幕候选\t判断")
	for _, it := range r.Items {
		yes := "否"
		if it.SubtitleCandidate {
			yes = "是"
		}
		fmt.Fprintf(t, "%s\t%s\t%s\t%s\n", displayName(it.Name), displayName(it.ID), yes, it.FilterReason)
	}
	t.Flush()
	for _, e := range r.Expected {
		fmt.Fprintf(w, "对照 %q：%s\n", e.Name, e.Conclusion)
	}
}
func saveReport(folder string, r Report, red *Redactor) error {
	// 报告中不保存 URL 或令牌；所有字符串仍经过统一脱敏，以兼容错误文本。
	data, e := json.MarshalIndent(r, "", "  ")
	if e != nil {
		return e
	}
	data = []byte(red.Text(string(data)))
	var safe Report
	if e = json.Unmarshal(data, &safe); e != nil {
		return e
	}
	data, e = json.MarshalIndent(safe, "", "  ")
	if e != nil {
		return e
	}
	if e = writeNew(filepath.Join(folder, "report.json"), data); e != nil {
		return e
	}
	var summary strings.Builder
	printReport(&summary, safe)
	if e = writeNew(filepath.Join(folder, "summary.txt"), []byte(summary.String())); e != nil {
		return e
	}
	var html strings.Builder
	if e = reportTemplate.Execute(&html, safe); e != nil {
		return e
	}
	return writeNew(filepath.Join(folder, "report.html"), []byte(html.String()))
}

var reportTemplate = template.Must(template.New("report").Parse(`<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>云盘文件检查报告</title><style>body{font:15px/1.7 system-ui,"Microsoft YaHei",sans-serif;background:#f3f5f8;color:#172336;margin:0;padding:36px}main{max-width:1200px;margin:auto;background:white;padding:32px;border-radius:16px}h1{margin:0 0 8px}table{width:100%;border-collapse:collapse;margin-top:24px}th,td{text-align:left;padding:12px;border-bottom:1px solid #e4e9f0;word-break:break-all}th{background:#eef3f9}code{background:#f4f6fa;padding:3px 6px}small{color:#57657b}.ok{color:#06764b}.warn{color:#a84f00}.box{background:#f4f7fb;padding:16px;border-radius:8px}.id{font-size:12px;max-width:250px}</style><main><h1>云盘文件检查报告</h1><small>{{.Time}} · {{.Version}}</small><p>模式 <code>{{.Mode}}</code>　云盘 <code>{{.Provider}}</code>　目录 <code>{{.Directory}}</code></p><div class="box"><b>接口返回 {{len .Items}} 个不同条目，读取 {{.Pages}} 页。</b><br>{{if .Complete}}<span class="ok">查询已按分页规则完成。</span>{{else}}<span class="warn">查询尚未完成，不能据此判定文件不存在。</span>{{end}} {{.StopReason}}{{if .Error}}<p class="warn">{{.Error}}</p>{{end}}</div><table><thead><tr><th>文件名</th><th>ID / fid</th><th>后缀</th><th>字节</th><th>爆米花字幕候选</th><th>判断</th></tr></thead><tbody>{{range .Items}}<tr><td>{{.Name}}</td><td class="id">{{.ID}}{{if .FID}}<br>{{.FID}}{{end}}</td><td>{{.Suffix}}</td><td>{{.Size}}</td><td>{{if .SubtitleCandidate}}是{{else}}否{{end}}</td><td>{{.FilterReason}}</td></tr>{{end}}</tbody></table>{{if .Expected}}<h2>预期文件对照</h2>{{range .Expected}}<p><b>{{.Name}}</b>：{{.Conclusion}}</p>{{end}}{{end}}<h2>解释范围</h2><ul>{{range .Notes}}<li>{{.}}</li>{{end}}</ul><p><small>本页为本地静态报告，不请求网络资源；原始 HTTP 记录另存为已脱敏的 http-*.json。</small></p></main></html>`))
