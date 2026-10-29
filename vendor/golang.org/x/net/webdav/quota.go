// CloudProbe local extension to golang.org/x/net/webdav v0.61.0.
// See third_party/webdav-quota.md for the patch's scope and maintenance notes.
package webdav

import (
	"context"
	"encoding/xml"
)

// QuotaFileSystem optionally exposes the protected live properties defined by
// RFC 4331. Values use bytes. QuotaSupported must not fetch remote capacity;
// transient lookup failures belong to Quota and are returned per property.
// Quota properties apply only to collections and are excluded from allprop
// unless explicitly requested with DAV:include.
type QuotaFileSystem interface {
	QuotaSupported(name string) bool
	Quota(ctx context.Context, name string) (usedBytes, availableBytes int64, err error)
}

var quotaProperties = []xml.Name{
	{Space: "DAV:", Local: "quota-used-bytes"},
	{Space: "DAV:", Local: "quota-available-bytes"},
}

func isQuotaProperty(name xml.Name) bool {
	return name == quotaProperties[0] || name == quotaProperties[1]
}
