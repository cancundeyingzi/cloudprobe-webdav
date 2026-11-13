package main

import (
	pathlist "container/list"
	"context"
	"errors"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	backupPathMaxEntries = 50000
	backupPathMaxBytes   = 16 << 20
)

var errBackupPathUnavailable = errors.New("OpenList 备用路径未建立或已失效，请重新解析目录；不能猜测云盘 ID 对应路径")

// A location is separate from the native object ID. Selected request nodes may
// outlive shared-cache eviction, but known removals and moves invalidate them.
type backupPathNode struct {
	owner                *backupDAVDrive
	ancestor             *backupPathNode
	id, parent, relative string
	directory            bool
	invalid              atomic.Bool
	parentVersion        uint64 // Both versions are guarded by owner.index.mu.
	childrenVersion      uint64
}

type backupPathRecord struct {
	node  *backupPathNode
	order *pathlist.Element
	bytes int
}

type backupPathIndex struct {
	mu                   sync.Mutex
	nodes                map[string]backupPathRecord
	order                pathlist.List
	bytes                int
	mapSlots             int
	active               map[*backupPathNode]map[<-chan struct{}]struct{}
	root                 *backupPathNode
	pending              map[string]*backupDirectoryPublication
	mutation             uint64
	maxEntries, maxBytes int // Zero selects the production limits.
}

// Publication ordering exists only while directory calls are outstanding. It
// cannot accumulate one historical counter for every directory ever visited.
type backupDirectoryPublication struct {
	readers           int
	issued, published uint64
}

type backupListingTicket struct {
	parent   string
	state    *backupDirectoryPublication
	order    uint64
	mutation uint64
}

func (b *backupDAVDrive) beginListing(parent string) backupListingTicket {
	b.index.mu.Lock()
	defer b.index.mu.Unlock()
	if b.index.pending == nil {
		b.index.pending = make(map[string]*backupDirectoryPublication)
	}
	state := b.index.pending[parent]
	if state == nil {
		state = &backupDirectoryPublication{}
		b.index.pending[parent] = state
	}
	state.readers++
	state.issued++
	return backupListingTicket{parent: parent, state: state, order: state.issued, mutation: b.index.mutation}
}

func (b *backupDAVDrive) finishListing(ticket backupListingTicket) {
	b.index.mu.Lock()
	defer b.index.mu.Unlock()
	ticket.state.readers--
	if ticket.state.readers == 0 {
		delete(b.index.pending, ticket.parent)
		if len(b.index.pending) == 0 {
			b.index.pending = nil
		}
	}
}

func backupPathNodeBytes(n *backupPathNode) int {
	bytes := 0
	for ; n != nil; n = n.ancestor {
		bytes += int(unsafe.Sizeof(backupPathNode{})) + len(n.id) + len(n.parent) + len(n.relative)
	}
	return bytes
}

func (x *backupPathIndex) remove(id string) {
	if record, ok := x.nodes[id]; ok {
		x.order.Remove(record.order)
		x.bytes -= record.bytes
		delete(x.nodes, id)
		if len(x.nodes) == 0 {
			// Go maps retain bucket allocations after delete. Drop the empty map
			// and its high-water reservation along with the final live record.
			x.nodes = nil
			x.bytes -= x.mapSlots * backupPathMapSlotBytes()
			x.mapSlots = 0
		}
	}
}

func backupPathMapSlotBytes() int {
	return int(unsafe.Sizeof(backupPathRecord{})) + int(unsafe.Sizeof("")) + 64
}

func (x *backupPathIndex) put(n *backupPathNode) {
	if existing, ok := x.nodes[n.id]; ok && existing.node == n {
		x.order.MoveToBack(existing.order)
		return
	}
	x.remove(n.id)
	maxEntries, maxBytes := x.maxEntries, x.maxBytes
	if maxEntries <= 0 {
		maxEntries = backupPathMaxEntries
	}
	if maxBytes <= 0 {
		maxBytes = backupPathMaxBytes
	}
	bytes := backupPathNodeBytes(n) + int(unsafe.Sizeof(pathlist.Element{}))
	if bytes+backupPathMapSlotBytes() > maxBytes {
		return
	}
	additionalMapBytes := func() int {
		if len(x.nodes) == x.mapSlots {
			return backupPathMapSlotBytes()
		}
		return 0
	}
	for len(x.nodes) >= maxEntries || x.bytes+bytes+additionalMapBytes() > maxBytes {
		x.remove(x.order.Front().Value.(string))
	}
	if x.nodes == nil {
		x.nodes = make(map[string]backupPathRecord)
	}
	if extra := additionalMapBytes(); extra != 0 {
		x.bytes += extra
		x.mapSlots++
	}
	x.nodes[n.id] = backupPathRecord{node: n, bytes: bytes, order: x.order.PushBack(n.id)}
	x.bytes += bytes
}

func (b *backupDAVDrive) rootLocation() *backupPathNode {
	b.index.mu.Lock()
	defer b.index.mu.Unlock()
	if b.index.root == nil {
		b.index.root = &backupPathNode{owner: b, id: b.root, directory: true}
	}
	return b.index.root
}

type backupParentKey struct{}

func (b *backupDAVDrive) parentContext(ctx context.Context, entry davEntry) context.Context {
	if entry.backupPath != nil {
		return context.WithValue(ctx, backupParentKey{}, entry.backupPath)
	}
	return ctx
}

func (b *backupDAVDrive) location(ctx context.Context, id string) (*backupPathNode, error) {
	if n, _ := ctx.Value(backupParentKey{}).(*backupPathNode); n != nil && n.owner == b && n.id == id {
		if n.usable() != nil {
			return nil, errBackupPathUnavailable
		}
		return n, nil
	}
	if id == b.root {
		return b.rootLocation(), nil
	}
	b.index.mu.Lock()
	defer b.index.mu.Unlock()
	if record, ok := b.index.nodes[id]; ok && record.node.usableLocked() == nil {
		b.index.order.MoveToBack(record.order)
		return record.node, nil
	}
	return nil, errBackupPathUnavailable
}

func (b *backupDAVDrive) relative(id string) (string, error) {
	n, err := b.location(context.Background(), id)
	if err != nil {
		return "", err
	}
	return n.relative, nil
}

// Only selected path nodes are pinned, never entire directory snapshots. The
// server cancels request contexts on completion, releasing their registrations.
func (b *backupDAVDrive) pin(ctx context.Context, n *backupPathNode) {
	if ctx.Done() == nil {
		return
	}
	b.index.mu.Lock()
	b.pinLocked(ctx, n)
	b.index.mu.Unlock()
}

func (b *backupDAVDrive) pinLocked(ctx context.Context, n *backupPathNode) {
	if ctx.Done() == nil {
		return
	}
	if b.index.active == nil {
		b.index.active = make(map[*backupPathNode]map[<-chan struct{}]struct{})
	}
	users := b.index.active[n]
	if users == nil {
		users = make(map[<-chan struct{}]struct{})
		b.index.active[n] = users
	}
	done := ctx.Done()
	if _, registered := users[done]; registered {
		return
	}
	users[done] = struct{}{}
	context.AfterFunc(ctx, func() {
		b.index.mu.Lock()
		delete(b.index.active[n], done)
		if len(b.index.active[n]) == 0 {
			delete(b.index.active, n)
			if len(b.index.active) == 0 {
				b.index.active = nil
			}
		}
		b.index.mu.Unlock()
	})
}

func (b *backupDAVDrive) bindLocation(ctx context.Context, parent, entry davEntry) (davEntry, error) {
	parentNode, err := b.location(b.parentContext(ctx, parent), parent.ID)
	if err != nil || !parentNode.directory {
		return entry, errBackupPathUnavailable
	}
	if n := entry.backupPath; n != nil && (n.owner != b || n.usable() != nil) {
		return entry, errBackupPathUnavailable
	}
	if safeFilename(entry.Name) != nil || entry.ID == "" {
		return entry, errBackupPathUnavailable
	}
	rel := path.Join(parentNode.relative, entry.Name)
	b.index.mu.Lock()
	defer b.index.mu.Unlock()
	if parentNode.usableLocked() != nil || entry.backupPath != nil && entry.backupPath.usableLocked() != nil {
		return entry, errBackupPathUnavailable
	}
	if entry.backupPath == nil || entry.backupPath.relative != rel || entry.backupPath.parent != parent.ID {
		entry.backupPath = &backupPathNode{owner: b, ancestor: parentNode, id: entry.ID, parent: parent.ID, relative: rel, directory: entry.Directory, parentVersion: parentNode.childrenVersion}
	}
	b.index.put(entry.backupPath)
	b.pinLocked(ctx, entry.backupPath)
	return entry, nil
}

func sameBackupLocation(n *backupPathNode, parent *backupPathNode, entry davEntry) bool {
	return n.parent == parent.id && n.relative == path.Join(parent.relative, entry.Name) && n.directory == entry.Directory
}

// Caller owns index.mu. Capacity eviction deliberately does not use this path.
func (b *backupDAVDrive) invalidateLocations(roots map[string]bool) {
	invalid := func(n *backupPathNode) bool {
		for rel := n.relative; rel != "." && rel != ""; rel = path.Dir(rel) {
			if roots[rel] {
				return true
			}
		}
		return false
	}
	for id, record := range b.index.nodes {
		if invalid(record.node) {
			record.node.invalid.Store(true)
			b.index.remove(id)
		}
	}
	for n := range b.index.active {
		if invalid(n) {
			n.invalid.Store(true)
		}
	}
}

func (b *backupDAVDrive) controlledLocationsLocked() map[*backupPathNode]bool {
	controlled := make(map[*backupPathNode]bool)
	collect := func(n *backupPathNode) {
		for ; n != nil; n = n.ancestor {
			if _, seen := controlled[n]; seen {
				break
			}
			controlled[n] = n.usableLocked() == nil
		}
	}
	for _, record := range b.index.nodes {
		collect(record.node)
	}
	for n := range b.index.active {
		collect(n)
	}
	return controlled
}

func (b *backupDAVDrive) publishLocationsLocked(parent *backupPathNode, controlled map[*backupPathNode]bool, removed map[string]bool) map[string]*backupPathNode {
	parent.childrenVersion++
	kept := make(map[string]*backupPathNode)
	for n, valid := range controlled {
		if valid && !n.invalid.Load() && !removed[n.relative] && n.ancestor == parent {
			n.parentVersion = parent.childrenVersion
			kept[n.id] = n
		}
	}
	return kept
}

// replace is true only for a complete listing. Upload/Mkdir results are deltas.
func (b *backupDAVDrive) recordLocations(parent *backupPathNode, entries []davEntry, replace bool, ticket backupListingTicket) error {
	b.index.mu.Lock()
	defer b.index.mu.Unlock()
	if ticket.state == nil {
		// A completed write supersedes earlier reads even when its retained
		// parent locator became stale before the result could be recorded.
		b.index.mutation++
		if parent != nil && parent.usableLocked() != nil {
			for candidate, valid := range b.controlledLocationsLocked() {
				if valid && candidate.directory && candidate.id == parent.id {
					parent = candidate
					break
				}
			}
		}
	}
	if parent != nil && parent.usableLocked() != nil || ticket.state != nil && (ticket.mutation != b.index.mutation || ticket.order < ticket.state.published) {
		unavailable := &backupPathNode{owner: b}
		unavailable.invalid.Store(true)
		for i := range entries {
			entries[i].backupPath = unavailable
		}
		return errBackupPathUnavailable
	}
	if ticket.state != nil {
		ticket.state.published = ticket.order
	}
	invalidListing := func() error {
		if parent != nil {
			parent.childrenVersion++
			roots := map[string]bool{}
			collect := func(n *backupPathNode) {
				for ; n != nil && n.id != b.root; n = n.ancestor {
					if n.parent == parent.id {
						roots[n.relative] = true
					}
				}
			}
			for _, record := range b.index.nodes {
				collect(record.node)
			}
			for n := range b.index.active {
				collect(n)
			}
			b.invalidateLocations(roots)
		}
		return errors.New("目录条目名称或 ID 无效、重复，不能建立 OpenList 备用定位")
	}
	byID := make(map[string]int, len(entries))
	byName := make(map[string]string, len(entries))
	for i, it := range entries {
		if it.ID == "" || safeFilename(it.Name) != nil || it.ID == b.root || parent != nil && it.ID == parent.id {
			return invalidListing()
		}
		if _, duplicate := byID[it.ID]; duplicate {
			return invalidListing()
		}
		if _, duplicate := byName[it.Name]; duplicate {
			return invalidListing()
		}
		byID[it.ID], byName[it.Name] = i, it.ID
	}
	if parent == nil {
		// Native identity checks can query a directory by ID without knowing
		// its path. Such results must never acquire a new path retroactively.
		unavailable := &backupPathNode{owner: b}
		unavailable.invalid.Store(true)
		for i := range entries {
			entries[i].backupPath = unavailable
		}
		return nil
	}
	// Retain only nodes still controlled by the shared index or a live request.
	// Detached directory-cache results observe the parent's version advance and
	// must be refreshed before they can establish a location again.
	controlled := b.controlledLocationsLocked()
	removed := map[string]bool{}
	for n, valid := range controlled {
		if !valid || n.id == b.root {
			continue
		}
		if i, exists := byID[n.id]; exists {
			if !sameBackupLocation(n, parent, entries[i]) {
				removed[n.relative] = true
			}
			continue
		}
		if n.parent == parent.id && (replace || byName[path.Base(n.relative)] != "") {
			removed[n.relative] = true
		}
	}
	b.invalidateLocations(removed)
	kept := b.publishLocationsLocked(parent, controlled, removed)
	for i := range entries {
		it := &entries[i]
		n := kept[it.ID]
		if n == nil || n.usableLocked() != nil || !sameBackupLocation(n, parent, *it) {
			n = &backupPathNode{owner: b, ancestor: parent, id: it.ID, parent: parent.id, relative: path.Join(parent.relative, it.Name), directory: it.Directory, parentVersion: parent.childrenVersion}
		}
		it.backupPath = n
		b.index.put(n)
	}
	return nil
}

func (b *backupDAVDrive) remember(parent string, entries []davEntry) {
	b.rememberEntries(parent, entries, false)
}

func (b *backupDAVDrive) rememberListing(parent string, entries []davEntry) {
	b.rememberEntries(parent, entries, true)
}

func (b *backupDAVDrive) rememberEntries(parent string, entries []davEntry, replace bool) {
	n, err := b.location(context.Background(), parent)
	if err != nil {
		b.index.mu.Lock()
		controlled := b.controlledLocationsLocked()
		for candidate, valid := range controlled {
			if valid && candidate.directory && candidate.id == parent {
				n = candidate
				break
			}
		}
		if n == nil {
			// No retained locator can prove this directory. Reject older reads
			// and detached snapshots without inventing an ID-to-path mapping.
			b.index.mutation++
			for candidate := range controlled {
				if candidate.directory {
					b.publishLocationsLocked(candidate, controlled, nil)
				}
			}
			if b.index.root != nil {
				if _, visited := controlled[b.index.root]; !visited {
					b.publishLocationsLocked(b.index.root, controlled, nil)
				}
			}
		}
		b.index.mu.Unlock()
	}
	if n != nil {
		_ = b.recordLocations(n, entries, replace, backupListingTicket{})
	}
}

func (b *backupDAVDrive) forget(it davEntry) {
	b.index.mu.Lock()
	defer b.index.mu.Unlock()
	b.index.mutation++
	roots := map[string]bool{}
	parents := map[*backupPathNode]bool{}
	controlled := b.controlledLocationsLocked()
	if it.backupPath != nil && it.backupPath.owner == b {
		roots[it.backupPath.relative] = true
		it.backupPath.invalid.Store(true)
		if it.backupPath.ancestor != nil {
			parents[it.backupPath.ancestor] = true
		}
	}
	for n := range controlled {
		if n.id == it.ID {
			roots[n.relative] = true
			n.invalid.Store(true)
			if n.ancestor != nil {
				parents[n.ancestor] = true
			}
		}
	}
	b.invalidateLocations(roots)
	if len(parents) == 0 {
		// No reliable location remains for this native ID. Invalidate detached
		// snapshots conservatively while preserving the controlled live paths.
		for n := range controlled {
			if n.directory {
				parents[n] = true
			}
		}
		if b.index.root != nil {
			parents[b.index.root] = true
		}
	}
	for parent := range parents {
		b.publishLocationsLocked(parent, controlled, roots)
	}
}

func (b *backupDAVDrive) readLocation(ctx context.Context, it davEntry) (*backupPathNode, error) {
	n := it.backupPath
	if n == nil {
		var err error
		n, err = b.location(ctx, it.ID)
		if err != nil {
			return nil, err
		}
	}
	if n.owner != b || n.usable() != nil || n.id != it.ID || path.Base(n.relative) != it.Name || n.directory != it.Directory {
		return nil, errBackupPathUnavailable
	}
	b.pin(ctx, n)
	return n, nil
}

func (n *backupPathNode) usable() error {
	if n == nil {
		return errBackupPathUnavailable
	}
	n.owner.index.mu.Lock()
	defer n.owner.index.mu.Unlock()
	return n.usableLocked()
}

func (n *backupPathNode) usableLocked() error {
	for ; n != nil; n = n.ancestor {
		if n.invalid.Load() || strings.HasPrefix(n.relative, "../") || n.ancestor != nil && n.parentVersion != n.ancestor.childrenVersion {
			return errBackupPathUnavailable
		}
	}
	return nil
}
