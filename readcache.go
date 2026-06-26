package nfs

import (
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-git/go-billy/v5"
)

// readHandle is an open, read-only fd kept across NFS READ requests so a run of
// reads to the same file reuses one fd instead of open+read+close per request.
// ReadAt is positional (pread) and safe for concurrent use, so reads take a
// read lock and can overlap; only close takes the write lock.
type readHandle struct {
	mu       sync.RWMutex
	file     billy.File
	closed   bool
	lastUsed atomic.Int64 // UnixNano, updated locklessly by readers
}

func (h *readHandle) readAt(p []byte, off int64) (int, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return 0, errHandleClosed
	}
	h.lastUsed.Store(time.Now().UnixNano())

	return h.file.ReadAt(p, off)
}

// stat fstats the open fd if supported, returning (info, true). Lets the read
// path build size/attrs without a worker-serialized path stat.
func (h *readHandle) stat() (os.FileInfo, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return nil, false
	}
	s, ok := h.file.(statter)
	if !ok {
		return nil, false
	}
	info, err := s.Stat()
	if err != nil {
		return nil, false
	}

	return info, true
}

func (h *readHandle) close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return nil
	}
	h.closed = true

	return h.file.Close()
}

// readCache holds open read-only handles keyed by NFS file handle, with an LRU
// bound and an idle sweeper. Concurrent reads of the same file share one fd.
type readCache struct {
	mu      sync.Mutex
	entries map[string]*readHandle
	maxSize int
	idle    time.Duration

	// gen counts attribute-invalidating removals (cross-connection drops from
	// SETATTR/REMOVE/RENAME/CREATE). readHandleFor samples gen before Open and
	// publishes via putIfFresh only if gen has not moved, so a read-only fd opened
	// under the old mode/size cannot be cached after an intervening invalidation —
	// which would otherwise serve pre-chmod/pre-truncate data. Guarded by mu.
	gen uint64

	stop chan struct{}
	once sync.Once
}

func newReadCache() *readCache {
	c := &readCache{
		entries: make(map[string]*readHandle),
		maxSize: defaultHandleCacheSize,
		idle:    defaultHandleIdle,
		stop:    make(chan struct{}),
	}
	go c.sweepLoop()

	return c
}

func (c *readCache) get(key string) *readHandle {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.entries[key]
}

func (c *readCache) put(key string, h *readHandle) {
	c.mu.Lock()
	victim := c.publishLocked(key, h)
	c.mu.Unlock()

	if victim != nil {
		_ = victim.close()
	}
}

// sampleGen returns the current invalidation generation, sampled before Open so
// putIfFresh can detect an invalidation that ran during the open window.
func (c *readCache) sampleGen() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.gen
}

// putIfFresh publishes h under key only if no invalidation has occurred since
// gen was sampled and no handle is already cached for key. On false the caller
// owns h and must close it. Mirrors writeCache.putIfFresh: it stops a read-only
// fd opened under now-stale attributes from being cached after a concurrent
// SETATTR/REMOVE/RENAME/CREATE dropped the handle.
func (c *readCache) putIfFresh(key string, h *readHandle, gen uint64) bool {
	c.mu.Lock()
	if c.gen != gen || c.entries[key] != nil {
		c.mu.Unlock()

		return false
	}
	victim := c.publishLocked(key, h)
	c.mu.Unlock()

	if victim != nil {
		_ = victim.close()
	}

	return true
}

// publishLocked inserts h under key, evicting the LRU entry if full. The caller
// holds c.mu; the returned victim (if any) must be closed outside the lock.
func (c *readCache) publishLocked(key string, h *readHandle) *readHandle {
	var victim *readHandle
	if _, exists := c.entries[key]; !exists && len(c.entries) >= c.maxSize {
		victim = c.evictOldestLocked()
	}
	c.entries[key] = h

	return victim
}

func (c *readCache) evictOldestLocked() *readHandle {
	var (
		oldestKey string
		oldest    *readHandle
		oldestAt  int64
	)
	for k, h := range c.entries {
		used := h.lastUsed.Load()
		if oldest == nil || used < oldestAt {
			oldestKey, oldest, oldestAt = k, h, used
		}
	}
	if oldest != nil {
		delete(c.entries, oldestKey)
	}

	return oldest
}

// removeIf detaches the handle for key only if it is still the given handle h,
// returning whether it did. A READ that observes errHandleClosed must drop the
// dead handle before retrying, but between the close and this call another READ
// may have cached a FRESH handle for the same key; an unconditional remove would
// detach that fresh fd without closing it, leaving it owned by no cache entry so
// neither drain nor idle sweep ever closes it (an fd leak). Comparing identity
// first means we only evict the handle we know is closed.
func (c *readCache) removeIf(key string, h *readHandle) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.entries[key] != h {
		return false
	}
	delete(c.entries, key)

	return true
}

// invalidate detaches and returns the handle for key (like remove) and bumps the
// invalidation generation so a read-only fd opened under now-stale attributes
// before this call cannot be published afterward (putIfFresh). Used by
// cross-connection drops (SETATTR/REMOVE/RENAME/CREATE). The gen is bumped even
// when nothing is cached: a racing open may not have published yet, and refusing
// it is the point.
func (c *readCache) invalidate(key string) *readHandle {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.gen++
	h := c.entries[key]
	delete(c.entries, key)

	return h
}

func (c *readCache) sweepLoop() {
	t := time.NewTicker(c.idle)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
			c.sweepIdle()
		}
	}
}

func (c *readCache) sweepIdle() {
	cutoff := time.Now().Add(-c.idle).UnixNano()
	var stale []*readHandle

	c.mu.Lock()
	for k, h := range c.entries {
		if h.lastUsed.Load() < cutoff {
			delete(c.entries, k)
			stale = append(stale, h)
		}
	}
	c.mu.Unlock()

	for _, h := range stale {
		_ = h.close()
	}
}

// Close closes every cached handle and stops the sweeper.
func (c *readCache) Close() {
	c.once.Do(func() { close(c.stop) })

	c.mu.Lock()
	all := c.entries
	c.entries = make(map[string]*readHandle)
	c.mu.Unlock()

	for _, h := range all {
		_ = h.close()
	}
}
