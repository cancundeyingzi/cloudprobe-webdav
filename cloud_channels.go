package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
)

// Each channel owns its object IDs. Translation uses the verified account and
// an unambiguous path; never pass website IDs to the mini-app API or vice versa.
type cloudChannel struct {
	name, root string
	drive      davDrive
}
type cloudChannelNode struct {
	source int
	entry  davEntry
	path   []string
}
type cloudChannelDrive struct {
	channels []cloudChannel
	mu       sync.Mutex
	nodes    map[string]cloudChannelNode
}

func (d *cloudChannelDrive) remember(source int, e davEntry, parent string, path []string) davEntry {
	key := fmt.Sprintf("channel%d:%s", source, e.ID)
	d.mu.Lock()
	d.nodes[key] = cloudChannelNode{source, e, append([]string(nil), path...)}
	d.mu.Unlock()
	e.ID = key
	e.Parent = parent
	return e
}
func (d *cloudChannelDrive) node(id string) (cloudChannelNode, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n, ok := d.nodes[id]
	return n, ok
}
func (d *cloudChannelDrive) resolve(ctx context.Context, id string, source int, write bool) (davEntry, []string, error) {
	c := d.channels[source]
	if id == "/" || id == "root" {
		return davEntry{Item: Item{ID: c.root, Directory: true}}, nil, nil
	}
	n, ok := d.node(id)
	if !ok {
		return davEntry{}, nil, errors.New("目录映射已失效，请刷新目录")
	}
	if n.source == source {
		return n.entry, n.path, nil
	}
	parent := c.root
	for i, name := range n.path {
		entries, err := c.drive.List(ctx, parent)
		if err != nil {
			return davEntry{}, nil, err
		}
		var found *davEntry
		for _, e := range entries {
			if e.Name == name {
				if found != nil {
					return davEntry{}, nil, errors.New("备用渠道存在重名对象，未自动关联")
				}
				copy := e
				found = &copy
			}
		}
		if found == nil {
			return davEntry{}, nil, errors.New("备用渠道尚未返回同路径对象")
		}
		if i == len(n.path)-1 {
			if found.Directory != n.entry.Directory || (!found.Directory && found.Size != n.entry.Size) {
				return davEntry{}, nil, errors.New("备用渠道对象与当前目录快照不一致")
			}
			if !found.Directory && write && (n.entry.ContentVersion == "" || found.ContentVersion != n.entry.ContentVersion) {
				return davEntry{}, nil, errors.New("备用写入渠道无法核验同一文件版本，请刷新后重试")
			}
			found.Parent = parent
			return *found, n.path, nil
		}
		if !found.Directory {
			return davEntry{}, nil, errors.New("备用渠道父路径不是目录")
		}
		parent = found.ID
	}
	return davEntry{}, nil, errors.New("备用渠道目录映射无效")
}
func (d *cloudChannelDrive) List(ctx context.Context, id string) ([]davEntry, error) {
	var failures []string
	for i, c := range d.channels {
		parent, path, err := d.resolve(ctx, id, i, false)
		if err == nil {
			var entries []davEntry
			entries, err = c.drive.List(ctx, parent.ID)
			if err == nil {
				out := make([]davEntry, 0, len(entries))
				for _, e := range entries {
					e.Parent = parent.ID
					out = append(out, d.remember(i, e, id, append(append([]string(nil), path...), e.Name)))
				}
				return out, nil
			}
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		failures = append(failures, c.name+"："+err.Error())
	}
	return nil, errors.New(strings.Join(failures, "；"))
}
func (d *cloudChannelDrive) ReadRoutes(ctx context.Context, e davEntry) []readRoute {
	routes := []readRoute{}
	for i, c := range d.channels {
		routes = append(routes, readRoute{Name: c.name, Resolve: func(ctx context.Context) (readTarget, error) {
			native, _, err := d.resolve(ctx, e.ID, i, false)
			if err != nil {
				return readTarget{}, err
			}
			u, err := c.drive.URL(ctx, native)
			return readTarget{URL: u}, err
		}})
	}
	return routes
}
func (d *cloudChannelDrive) URL(ctx context.Context, e davEntry) (string, error) {
	var err error
	for _, r := range d.ReadRoutes(ctx, e) {
		var t readTarget
		t, err = r.Resolve(ctx)
		if err == nil {
			return t.URL, nil
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
	}
	return "", err
}
func channelMayRetry(err error) bool {
	var not *davNotExecuted
	return errors.As(err, &not) || confirmedMutationRejection(err)
}
func (d *cloudChannelDrive) create(ctx context.Context, parent, name string, file *os.File) (davEntry, error) {
	var last error
	for i, c := range d.channels {
		p, path, err := d.resolve(ctx, parent, i, true)
		if err != nil {
			last = err
			continue
		}
		var e davEntry
		if file == nil {
			e, err = c.drive.Mkdir(ctx, p.ID, name)
		} else {
			e, err = c.drive.Upload(ctx, p.ID, name, file)
		}
		if err == nil {
			e.Parent = p.ID
			return d.remember(i, e, parent, append(append([]string(nil), path...), name)), nil
		}
		last = err
		if !channelMayRetry(err) || ctx.Err() != nil {
			return davEntry{}, err
		}
	}
	return davEntry{}, last
}
func (d *cloudChannelDrive) Mkdir(ctx context.Context, p, n string) (davEntry, error) {
	return d.create(ctx, p, n, nil)
}
func (d *cloudChannelDrive) Upload(ctx context.Context, p, n string, f *os.File) (davEntry, error) {
	return d.create(ctx, p, n, f)
}
func (d *cloudChannelDrive) Remove(ctx context.Context, e davEntry) error {
	var last error
	for i, c := range d.channels {
		native, _, err := d.resolve(ctx, e.ID, i, true)
		if err != nil {
			last = err
			continue
		}
		err = c.drive.Remove(ctx, native)
		if err == nil {
			return nil
		}
		last = err
		if !channelMayRetry(err) || ctx.Err() != nil {
			return err
		}
	}
	return last
}
func (d *cloudChannelDrive) Move(ctx context.Context, e davEntry, parent, name string) error {
	var last error
	for i, c := range d.channels {
		native, _, err := d.resolve(ctx, e.ID, i, true)
		if err != nil {
			last = err
			continue
		}
		p, path, err := d.resolve(ctx, parent, i, true)
		if err != nil {
			last = err
			continue
		}
		err = c.drive.Move(ctx, native, p.ID, name)
		if err == nil {
			native.Parent = p.ID
			native.Name = name
			d.mu.Lock()
			d.nodes[e.ID] = cloudChannelNode{i, native, append(append([]string(nil), path...), name)}
			d.mu.Unlock()
			return nil
		}
		last = err
		if !channelMayRetry(err) || ctx.Err() != nil {
			return err
		}
	}
	return last
}
