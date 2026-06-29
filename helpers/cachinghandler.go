package helpers

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io/fs"
	"sync"

	"github.com/willscott/go-nfs"

	"github.com/go-git/go-billy/v5"
	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
)

// NewCachingHandler wraps a handler to provide a basic to/from-file handle cache.
func NewCachingHandler(h nfs.Handler, limit int) nfs.Handler {
	return NewCachingHandlerWithVerifierLimit(h, limit, limit)
}

// NewCachingHandlerWithVerifierLimit provides a basic to/from-file handle cache that can be tuned with a smaller cache of active directory listings.
func NewCachingHandlerWithVerifierLimit(h nfs.Handler, limit int, verifierLimit int) nfs.Handler {
	if limit < 2 || verifierLimit < 2 {
		nfs.Log.Warnf("Caching handler created with insufficient cache to support directory listing", "size", limit, "verifiers", verifierLimit)
	}
	verifiers, _ := lru.New[uint64, verifier](verifierLimit)
	c := &CachingHandler{
		Handler:         h,
		reverseHandles:  make(map[string][]uuid.UUID),
		activeVerifiers: verifiers,
		cacheLimit:      limit,
	}
	// Register the eviction callback with the LRU itself rather than reading
	// GetOldest before Add: the library invokes onCacheEvict with the entry it
	// actually evicted, under its own lock, so concurrent ToHandle calls cannot
	// make us clean up (or drop the fd for) the wrong handle.
	c.activeHandles, _ = lru.NewWithEvict[uuid.UUID, entry](limit, c.onCacheEvict)

	return c
}

// CachingHandler implements to/from handle via an LRU cache.
type CachingHandler struct {
	nfs.Handler
	activeHandles    *lru.Cache[uuid.UUID, entry]
	reverseHandles   map[string][]uuid.UUID
	reverseHandlesMu sync.RWMutex
	activeVerifiers  *lru.Cache[uint64, verifier]
	cacheLimit       int

	// onEvict, if set by the server via SetHandleEvictionCallback, is invoked with
	// the opaque handle of every entry evicted from activeHandles, so the server
	// can close any per-connection fd still cached under it (see
	// nfs.HandleEvictionReceiver). nil when the server has not registered one.
	onEvictMu sync.RWMutex
	onEvict   func(handle []byte)
}

type entry struct {
	f billy.Filesystem
	p []string
}

// ToHandle takes a file and represents it with an opaque handle to reference it.
// In stateless nfs (when it's serving a unix fs) this can be the device + inode
// but we can generalize with a stateful local cache of handed out IDs.
func (c *CachingHandler) ToHandle(ctx context.Context, f billy.Filesystem, path []string) []byte {
	joinedPath := f.Join(path...)

	if handle := c.searchReverseCache(f, joinedPath); handle != nil {
		return handle
	}

	id := uuid.New()

	newPath := make([]string, len(path))

	copy(newPath, path)
	// Add may evict the oldest entry; onCacheEvict (registered with the LRU) runs
	// for the entry actually evicted and handles both reverse-cache cleanup and
	// the fd-eviction notification, so there is no GetOldest/Add race here.
	c.activeHandles.Add(id, entry{f, newPath})

	c.appendReverseHandle(joinedPath, id)
	b, _ := id.MarshalBinary()

	return b
}

// onCacheEvict is the LRU's eviction callback, invoked (outside the cache lock)
// for the entry the cache actually removed — whether by LRU pressure during Add
// or by an explicit Remove. It drops the entry's reverse-cache mapping and tells
// the server to close any per-connection fd still cached under the handle, so a
// later RENAME/REMOVE that can no longer resolve the path to this handle does not
// leave an fd open (failing rename/remove on Windows-like backends).
func (c *CachingHandler) onCacheEvict(id uuid.UUID, e entry) {
	rk := e.f.Join(e.p...)
	c.evictReverseCache(rk, id)
	c.notifyEvicted(id)
}

// SetHandleEvictionCallback registers fn to be called with the opaque handle of
// each entry evicted from the handle cache, so the server can close any
// per-connection fd cached under it. Implements nfs.HandleEvictionReceiver.
func (c *CachingHandler) SetHandleEvictionCallback(fn func(handle []byte)) {
	c.onEvictMu.Lock()
	defer c.onEvictMu.Unlock()

	c.onEvict = fn
}

// notifyEvicted invokes the registered eviction callback (if any) with the
// handle's wire bytes. Called after the handle has left activeHandles, with no
// cache lock held, so the callback may freely drop fds.
func (c *CachingHandler) notifyEvicted(id uuid.UUID) {
	c.onEvictMu.RLock()
	fn := c.onEvict
	c.onEvictMu.RUnlock()

	if fn != nil {
		b, _ := id.MarshalBinary()
		fn(b)
	}
}

// FromHandle converts from an opaque handle to the file it represents
func (c *CachingHandler) FromHandle(ctx context.Context, fh []byte) (billy.Filesystem, []string, error) {
	id, err := uuid.FromBytes(fh)
	if err != nil {
		return nil, []string{}, err
	}

	if f, ok := c.activeHandles.Get(id); ok {
		// Re-pin the LRU recency of this handle's ancestors so a parent directory
		// handle is not evicted while a child is still in use (which would hand the
		// client a stale parent). Resolve ancestors directly through the reverse
		// map — O(path depth) — instead of scanning every cached entry, which made
		// FromHandle O(N) on a hot path taken by every WRITE/READ/COMMIT and
		// serialized concurrent requests on the cache's internal lock.
		c.repinAncestors(f.f, f.p)
		newP := make([]string, len(f.p))
		copy(newP, f.p)
		return f.f, newP, nil
	}
	return nil, []string{}, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
}

// repinAncestors bumps the LRU recency of every cached handle for an ancestor
// (root, parent, grandparent, ...) of path, so a directory handle is not evicted
// from under a child handle that is still being accessed. Ancestor ids are
// gathered from the reverse-path map in O(path depth) rather than scanning the
// whole handle cache. The reverse-map ids are collected under its read lock and
// the lock is released before touching activeHandles, preserving the activeHandles
// -> reverseHandlesMu lock order used by ToHandle's eviction callback.
func (c *CachingHandler) repinAncestors(f billy.Filesystem, path []string) {
	c.reverseHandlesMu.RLock()
	var ids []uuid.UUID
	for i := 0; i < len(path); i++ {
		ancestor := f.Join(path[:i]...)
		ids = append(ids, c.reverseHandles[ancestor]...)
	}
	c.reverseHandlesMu.RUnlock()

	for _, id := range ids {
		_, _ = c.activeHandles.Get(id)
	}
}

// allocating or publishing a new one, or nil if none is currently cached. It
// lets handle-mutating ops (RENAME) invalidate cached fds for a path without the
// side effects of ToHandle (minting a fresh handle that a concurrent LOOKUP
// could observe and then find stale, or evicting a live handle on a full cache).
// A cached fd can only exist under a handle the client already looked up, so a
// nil result means there is nothing to invalidate for that path.
func (c *CachingHandler) HandleForPathIfCached(f billy.Filesystem, path []string) []byte {
	return c.searchReverseCache(f, f.Join(path...))
}

func (c *CachingHandler) searchReverseCache(f billy.Filesystem, path string) []byte {
	// Hold RLock for entire iteration to prevent races with appendReverseHandle
	// and evictReverseCache which modify the slice. This is safe because
	// activeHandles.Get() has its own internal synchronization (LRU cache).
	c.reverseHandlesMu.RLock()
	defer c.reverseHandlesMu.RUnlock()

	for _, id := range c.reverseHandles[path] {
		if candidate, ok := c.activeHandles.Get(id); ok {
			// Use interface comparison instead of reflect.DeepEqual to avoid
			// race conditions. reflect.DeepEqual traverses all internal fields
			// of the filesystem, including mutable maps that can be modified
			// concurrently by file operations. Interface comparison (==) only
			// compares type and pointer, which is sufficient for checking if
			// it's the same filesystem instance.
			if candidate.f == f {
				return id[:]
			}
		}
	}

	return nil
}

func (c *CachingHandler) evictReverseCache(path string, handle uuid.UUID) {
	c.reverseHandlesMu.Lock()
	defer c.reverseHandlesMu.Unlock()

	uuids, ok := c.reverseHandles[path]
	if !ok {
		return
	}
	for i, u := range uuids {
		if u == handle {
			c.reverseHandles[path] = append(uuids[:i], uuids[i+1:]...)
			return
		}
	}
}

func (c *CachingHandler) appendReverseHandle(path string, id uuid.UUID) {
	c.reverseHandlesMu.Lock()
	defer c.reverseHandlesMu.Unlock()
	c.reverseHandles[path] = append(c.reverseHandles[path], id)
}

func (c *CachingHandler) InvalidateHandle(ctx context.Context, fs billy.Filesystem, handle []byte) error {
	// Remove fires the LRU eviction callback (onCacheEvict) for the entry it
	// actually removes, which drops the reverse-cache mapping and the fd, so no
	// separate cleanup is needed here.
	id, _ := uuid.FromBytes(handle)
	c.activeHandles.Remove(id)
	return nil
}

// HandleLimit exports how many file handles can be safely stored by this cache.
func (c *CachingHandler) HandleLimit() int {
	return c.cacheLimit
}

type verifier struct {
	path     string
	contents []fs.FileInfo
}

func hashPathAndContents(path string, contents []fs.FileInfo) uint64 {
	//calculate a cookie-verifier.
	vHash := sha256.New()

	// Add the path to avoid collisions of directories with the same content
	vHash.Write(binary.BigEndian.AppendUint64([]byte{}, uint64(len(path))))
	vHash.Write([]byte(path))

	for _, c := range contents {
		vHash.Write([]byte(c.Name())) // Never fails according to the docs
	}

	verify := vHash.Sum(nil)[0:8]
	return binary.BigEndian.Uint64(verify)
}

func (c *CachingHandler) VerifierFor(path string, contents []fs.FileInfo) uint64 {
	id := hashPathAndContents(path, contents)
	c.activeVerifiers.Add(id, verifier{path, contents})
	return id
}

func (c *CachingHandler) DataForVerifier(path string, id uint64) []fs.FileInfo {
	if cache, ok := c.activeVerifiers.Get(id); ok {
		return cache.contents
	}
	return nil
}
