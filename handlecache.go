package nfs

import (
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5"
)

// errHandleClosed is returned by a cachedHandle whose file was closed (evicted)
// concurrently with a write. The caller should re-open and retry once.
var errHandleClosed = errors.New("cached write handle closed")

// syncer is the optional capability a billy.File may expose to flush buffered
// data to stable storage without closing the file. os.File implements it; the
// e2b chroot wrappedFile forwards it. When absent, COMMIT degrades to closing
// the handle (which also flushes) on the next eviction.
type syncer interface{ Sync() error }

// writerAt is the optional positional-write capability a billy.File may expose.
type writerAt interface {
	WriteAt([]byte, int64) (int, error)
}

// statter is the optional capability a billy.File may expose to fstat the open
// fd directly. When present, the proxy builds write-cache (wcc) attributes from
// it instead of a path-based Stat, avoiding the backing filesystem's
// (potentially serialized) metadata path on every WRITE.
type statter interface{ Stat() (os.FileInfo, error) }

// defaultHandleCacheSize bounds the number of files kept open for unstable
// writes. Each entry is one open fd on the backing filesystem.
const defaultHandleCacheSize = 256

// defaultHandleIdle is how long a handle may sit unused before it is flushed
// and closed by the background sweeper.
const defaultHandleIdle = 30 * time.Second

// cachedHandle is a single open file kept across NFS WRITE requests so that a
// run of unstable writes to the same file reuses one fd instead of paying an
// open+write+close (and the backing store's per-close flush) on every request.
type cachedHandle struct {
	mu sync.RWMutex // excludes Sync/Close from active positional writes

	file billy.File

	stateMu  sync.Mutex
	dirty    bool      // unflushed unstable writes are present
	lastUsed time.Time // for idle eviction
	closed   bool
}

// writeAt writes data at offset, marking the handle dirty. Backends exposing
// WriteAt can process independent writes concurrently; older billy.File
// implementations fall back to serialized Seek+Write because the fd offset is
// shared.
func (h *cachedHandle) writeAt(data []byte, offset int64) (int, error) {
	if wa, ok := h.file.(writerAt); ok {
		h.mu.RLock()
		defer h.mu.RUnlock()

		if h.closed {
			return 0, errHandleClosed
		}
		start := time.Now()
		n, err := wa.WriteAt(data, offset)
		writeProfile.recordBackendWrite(time.Since(start).Nanoseconds(), n)
		if n > 0 {
			h.markDirty()
		}

		return n, err
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return 0, errHandleClosed
	}
	if _, err := h.file.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	}
	start := time.Now()
	n, err := h.file.Write(data)
	writeProfile.recordBackendWrite(time.Since(start).Nanoseconds(), n)
	if n > 0 {
		h.markDirty()
	}

	return n, err
}

func (h *cachedHandle) markDirty() {
	h.stateMu.Lock()
	h.dirty = true
	h.lastUsed = time.Now()
	h.stateMu.Unlock()
}

func (h *cachedHandle) markClean() {
	h.stateMu.Lock()
	h.dirty = false
	h.stateMu.Unlock()
}

func (h *cachedHandle) dirtyState() bool {
	h.stateMu.Lock()
	defer h.stateMu.Unlock()

	return h.dirty
}

func (h *cachedHandle) lastUsedState() time.Time {
	h.stateMu.Lock()
	defer h.stateMu.Unlock()

	return h.lastUsed
}

// stat fstats the open fd if the file supports it, returning (info, true). When
// the file has no Stat method it returns (nil, false) and the caller falls back
// to a path-based stat.
func (h *cachedHandle) stat() (os.FileInfo, bool) {
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

// sync flushes buffered data to stable storage if the file supports it. After a
// successful sync the handle is no longer dirty. Returns false (without error)
// only when the backing file cannot sync without closing.
func (h *cachedHandle) sync() (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return false, errHandleClosed
	}
	s, ok := h.file.(syncer)
	if !ok {
		return false, nil
	}
	start := time.Now()
	err := s.Sync()
	writeProfile.recordSync(time.Since(start).Nanoseconds())
	if err != nil {
		return true, err
	}
	h.markClean()

	return true, nil
}

// syncIfDirty flushes buffered data to stable storage only when the handle
// holds unflushed unstable writes, so a separate read-only fd opened from the
// backing store observes them (read-after-unstable-write coherence). It returns
// dirty=false when there is nothing to flush (clean). closed=true means a
// concurrent eviction/drop already closed the handle: its close also flushed,
// but that close may have FAILED (ENOSPC/EIO), so the caller must consult the
// commit tracker rather than assume the data reached stable storage. When dirty
// and not closed, synced=false (without error) means the backing file cannot
// sync without closing and the caller must close-flush instead.
func (h *cachedHandle) syncIfDirty() (dirty bool, synced bool, closed bool, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return false, false, true, nil
	}
	if !h.dirtyState() {
		return false, false, false, nil
	}
	s, ok := h.file.(syncer)
	if !ok {
		return true, false, false, nil
	}
	start := time.Now()
	err = s.Sync()
	writeProfile.recordSync(time.Since(start).Nanoseconds())
	if err != nil {
		return true, true, false, err
	}
	h.markClean()

	return true, true, false, nil
}

// closeFlush closes the underlying file, which also flushes any buffered data.
func (h *cachedHandle) closeFlush() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return nil
	}
	h.closed = true
	h.markClean()

	return h.file.Close()
}

// evictFlush closes the handle during cache eviction (LRU/idle/drain). It
// reports lostDirty=true when the handle still had unflushed unstable writes
// AND the close-flush failed, i.e. acknowledged-but-not-yet-committed data may
// have been lost (ENOSPC/EIO). The caller records that error so a later COMMIT
// for the file surfaces it instead of falsely succeeding via commitByPath.
func (h *cachedHandle) evictFlush() (lostDirty bool, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return false, nil
	}
	h.closed = true
	wasDirty := h.dirtyState()
	h.markClean()
	err = h.file.Close()

	return wasDirty && err != nil, err
}

// writeCache holds open handles for in-flight unstable writes, keyed by the
// opaque NFS file handle. It is safe for concurrent use. A background sweeper
// flushes and evicts idle handles so data does not sit unflushed indefinitely
// when a client never sends COMMIT.
type writeCache struct {
	mu      sync.Mutex
	entries map[string]*cachedHandle
	maxSize int
	idle    time.Duration

	// gen counts invalidations (any removal of a handle: eviction, idle sweep,
	// drain, or a cross-connection drop from SETATTR/REMOVE/RENAME/CREATE). The
	// open-then-publish path in cachedWrite samples gen before OpenFile and only
	// publishes the fd if gen has not moved (putIfFresh), so an fd opened under
	// the old mode/size cannot be cached *after* an invalidation that ran during
	// the open window — which would otherwise let later writes reuse a stale fd,
	// bypassing a chmod or re-extending past a truncate. Guarded by mu.
	gen uint64

	// commits is the mount-wide (Server-scoped) durability tracker. When this
	// cache evicts or drains a still-dirty handle whose close-flush fails, the
	// error is recorded here so a COMMIT on ANY nconnect connection — not just
	// this one — surfaces it instead of falsely succeeding via commitByPath. An
	// eviction publishes a pending marker (begin) atomically with removing the
	// handle from entries, closing the window in which a peer COMMIT would miss
	// both the live handle and the not-yet-recorded error.
	commits *commitTracker

	stop chan struct{}
	once sync.Once
}

func newWriteCache(commits *commitTracker) *writeCache {
	if commits == nil {
		// Standalone use (tests): a private tracker keeps the cache self-contained.
		commits = newCommitTracker()
	}
	c := &writeCache{
		entries: make(map[string]*cachedHandle),
		commits: commits,
		maxSize: defaultHandleCacheSize,
		idle:    defaultHandleIdle,
		stop:    make(chan struct{}),
	}
	go c.sweepLoop()

	return c
}

// get returns the cached handle for key, or nil if absent.
func (c *writeCache) get(key string) *cachedHandle {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.entries[key]
}

// isCurrent reports whether h is still the cached handle for key. Hints captured
// before a handle invalidation must be revalidated before use, otherwise a racing
// writer could keep using an fd that has already been detached from the cache.
func (c *writeCache) isCurrent(key string, h *cachedHandle) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return h != nil && c.entries[key] == h
}

func (c *writeCache) getCurrent(key string, hint *cachedHandle) *cachedHandle {
	if hint != nil && c.isCurrent(key, hint) {
		return hint
	}

	return c.get(key)
}

// sampleGen returns the current invalidation generation. The open path samples
// this before OpenFile and passes it to putIfFresh so an fd opened under
// now-stale attributes is not cached after an intervening invalidation.
func (c *writeCache) sampleGen() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.gen
}

// put stores h under key, evicting the oldest entry if the cache is full.
func (c *writeCache) put(key string, h *cachedHandle) {
	c.mu.Lock()
	victimKey, victim, done := c.publishLocked(key, h)
	c.mu.Unlock()

	if victim != nil {
		lost, err := victim.evictFlush()
		c.commits.finish(victimKey, done, lostErr(lost, err))
	}
}

// putIfFresh publishes h under key only if no invalidation has occurred since
// gen was sampled (c.gen unchanged) and no handle is already cached for key.
// This closes the open-then-publish race: a WRITE/READ that opened an fd under
// the old mode/size before a concurrent SETATTR/REMOVE/RENAME/CREATE dropped the
// handle must not cache that stale fd afterward. Returns true if h was
// published; on false the caller owns h and must close it (and reopen). The gen
// check and insert happen under one lock so an invalidation cannot slip between.
func (c *writeCache) putIfFresh(key string, h *cachedHandle, gen uint64) bool {
	c.mu.Lock()
	if c.gen != gen || c.entries[key] != nil {
		c.mu.Unlock()

		return false
	}
	victimKey, victim, done := c.publishLocked(key, h)
	c.mu.Unlock()

	if victim != nil {
		lost, err := victim.evictFlush()
		c.commits.finish(victimKey, done, lostErr(lost, err))
	}

	return true
}

// publishLocked inserts h under key, evicting the LRU entry if the cache is
// full. The caller holds c.mu. Returns the evicted victim (or nil) and its
// pending-marker channel, which the caller must flush+finish outside the lock. A
// prior eviction flush error for key is NOT cleared: re-caching a fresh handle
// does not recover bytes already lost on a failed close, so the recorded error
// must survive until a COMMIT surfaces it.
func (c *writeCache) publishLocked(key string, h *cachedHandle) (string, *cachedHandle, chan struct{}) {
	var (
		victim    *cachedHandle
		victimKey string
		done      chan struct{}
	)
	if _, exists := c.entries[key]; !exists && len(c.entries) >= c.maxSize {
		victimKey, victim = c.evictOldestLocked()
		if victim != nil {
			// Publish the pending marker while still holding c.mu, atomically with
			// the victim leaving entries (evictOldestLocked deleted it). This closes
			// the window in which a peer COMMIT would find neither the live handle
			// nor a recorded result and fall through to commitByPath.
			done = c.commits.begin(victimKey)
		}
	}
	c.entries[key] = h

	return victimKey, victim, done
}

// lostErr returns err only when an eviction flush actually lost still-dirty
// (acknowledged-but-uncommitted) data; a clean handle whose close fails has
// nothing unflushed to lose and must not be recorded as a commit failure.
func lostErr(lost bool, err error) error {
	if lost {
		return err
	}

	return nil
}

// flushDirty makes any cached handle for key durable if (and only if) it holds
// unflushed unstable writes, so a separate read-only fd sees them. Returns
// whether a dirty handle was found and flushed (so the caller need not look
// elsewhere) and the first flush error. When the backing file cannot Sync
// without closing, it closes-and-removes the handle so the next write reopens.
func (c *writeCache) flushDirty(key string) (flushed bool, err error) {
	h := c.get(key)
	if h == nil {
		// No live handle. It may still have been evicted/dropped a moment ago with
		// its close-flush in flight or already failed (LRU/idle/drain/drop remove
		// the handle from entries before — and complete the flush after — leaving
		// the lock). On a no-Sync/buffered backend that close is what makes the
		// unstable writes durable, so a READ here must not open a fresh read-only
		// fd and serve stale bytes, nor hide a recorded ENOSPC/EIO. Consult the
		// mount-wide tracker: await (non-consuming) blocks for any in-flight close
		// and surfaces a recorded loss without clearing it, so the client's later
		// COMMIT for the same writes still observes it.
		if cerr := c.commits.await(key); cerr != nil {
			return true, cerr
		}
		return false, nil
	}

	dirty, synced, closed, serr := h.syncIfDirty()
	if closed {
		// A concurrent eviction/drop already closed this handle. That close also
		// flushed it, but may have FAILED with the only copy of an acknowledged
		// unstable write — recorded on the mount-wide tracker. Surface that error
		// so the READ does not proceed to serve stale data as if the write were
		// durable. await blocks for any in-flight close and returns the loss
		// WITHOUT consuming it: COMMIT, not READ, is the checkpoint that resolves
		// the error, so a later COMMIT for these writes must still see it.
		if cerr := c.commits.await(key); cerr != nil {
			return true, cerr
		}
		return false, nil
	}
	if !dirty {
		return false, nil
	}
	if serr != nil {
		return true, serr
	}
	if !synced {
		// No Sync() capability: close to force a flush, then drop so the next
		// write reopens. Publish a tracker marker via removeAndTrack before the
		// handle leaves the cache and finish it with the close result: the READ
		// that triggered this flush returns the error, but the handle is now
		// detached, so a later or racing COMMIT for the same file must still
		// surface a lost-dirty close failure via the tracker instead of falsely
		// succeeding through commitByPath. Close the handle removeAndTrack actually
		// detached (a concurrent put may have replaced the one we synced), and use
		// evictFlush so only a still-dirty close failure is recorded as a loss.
		if dropped, done := c.removeAndTrack(key); dropped != nil {
			lost, cerr := dropped.evictFlush()
			c.commits.finish(key, done, lostErr(lost, cerr))
			if cerr != nil {
				return true, cerr
			}
		}
	}

	return true, nil
}

// takeCommitErr awaits any in-flight eviction flush for key and returns/clears
// the resulting error, delegating to the mount-wide tracker so a flush failure
// recorded on another connection is surfaced here. Used by the COMMIT path
// before falling back to commitByPath.
func (c *writeCache) takeCommitErr(key string) error {
	return c.commits.wait(key)
}

// evictOldestLocked removes and returns the least-recently-used handle and its
// key. The caller holds c.mu; the returned handle must be closed outside the
// lock. Each handle's lastUsed is read through its own state lock because
// writeAt mutates lastUsed there, not under c.mu; reading it bare here would race
// a concurrent write to a different file that fills the cache.
func (c *writeCache) evictOldestLocked() (string, *cachedHandle) {
	var (
		oldestKey  string
		oldest     *cachedHandle
		oldestUsed time.Time
	)
	for k, h := range c.entries {
		used := h.lastUsedState()
		if oldest == nil || used.Before(oldestUsed) {
			oldestKey, oldest, oldestUsed = k, h, used
		}
	}
	if oldest != nil {
		delete(c.entries, oldestKey)
	}

	return oldestKey, oldest
}

// remove detaches and returns the handle for key without closing it.
func (c *writeCache) remove(key string) *cachedHandle {
	c.mu.Lock()
	defer c.mu.Unlock()

	h := c.entries[key]
	delete(c.entries, key)

	return h
}

// removeIf detaches the handle for key only if it is still the given handle h,
// returning whether it did. A WRITE that observes errHandleClosed must drop the
// dead handle before retrying, but between the close and this call another WRITE
// may have published a FRESH handle for the same key; an unconditional remove
// would detach that fresh fd, leaving its unstable writes unreachable to
// COMMIT/drain. Comparing identity first means we only evict the handle we know
// is closed and never the live replacement.
func (c *writeCache) removeIf(key string, h *cachedHandle) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.entries[key] != h {
		return false
	}
	delete(c.entries, key)

	return true
}

// removeAndTrack detaches the handle for key and, if one was present, publishes
// a pending eviction marker for it under the same lock (so a peer COMMIT in the
// flush window observes the eviction rather than racing to commitByPath). The
// caller must call commits.finish with the returned channel once it has flushed
// the handle. Returns (nil, nil) when no handle was cached.
func (c *writeCache) removeAndTrack(key string) (*cachedHandle, chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	h := c.entries[key]
	if h == nil {
		return nil, nil
	}
	delete(c.entries, key)

	return h, c.commits.begin(key)
}

// invalidate detaches the handle for key like removeAndTrack and additionally
// bumps the invalidation generation, so an fd opened under now-stale attributes
// before this call cannot be published into the cache afterward (putIfFresh).
// Used by cross-connection drops (SETATTR/REMOVE/RENAME/CREATE) where the file's
// mode or size is changing — unlike LRU/idle eviction or a flush-by-close, those
// genuinely invalidate the attributes a concurrently-opening fd was checked
// against. The gen is bumped even when no handle is cached: the racing open may
// not have published yet, and refusing it is the whole point.
func (c *writeCache) invalidate(key string) (*cachedHandle, chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.gen++
	h := c.entries[key]
	if h == nil {
		return nil, nil
	}
	delete(c.entries, key)

	return h, c.commits.begin(key)
}

func (c *writeCache) sweepLoop() {
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

// sweepIdle flushes and closes handles idle longer than c.idle.
func (c *writeCache) sweepIdle() {
	cutoff := time.Now().Add(-c.idle)
	type staleEntry struct {
		key  string
		h    *cachedHandle
		done chan struct{}
	}
	var stale []staleEntry

	c.mu.Lock()
	for k, h := range c.entries {
		idle := h.lastUsedState().Before(cutoff)
		if idle {
			delete(c.entries, k)
			// Mark pending under c.mu, atomically with leaving entries, so a peer
			// COMMIT in the flush window observes the eviction (see put).
			stale = append(stale, staleEntry{k, h, c.commits.begin(k)})
		}
	}
	c.mu.Unlock()

	for _, e := range stale {
		lost, err := e.h.evictFlush()
		c.commits.finish(e.key, e.done, lostErr(lost, err))
	}
}

// Close flushes and closes every cached handle and stops the sweeper.
func (c *writeCache) Close() {
	c.once.Do(func() { close(c.stop) })

	type drained struct {
		key  string
		h    *cachedHandle
		done chan struct{}
	}

	c.mu.Lock()
	all := make([]drained, 0, len(c.entries))
	for k, h := range c.entries {
		// Publish pending before the handle leaves the cache: with nconnect a peer
		// connection can still COMMIT this file after this connection drains, and
		// must observe a flush failure rather than a false success.
		all = append(all, drained{k, h, c.commits.begin(k)})
	}
	c.entries = make(map[string]*cachedHandle)
	c.mu.Unlock()

	for _, e := range all {
		lost, err := e.h.evictFlush()
		c.commits.finish(e.key, e.done, lostErr(lost, err))
	}
}
