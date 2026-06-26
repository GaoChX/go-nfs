package nfs

import (
	"errors"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5/osfs"
)

// P2: an nconnect connection that drains (disconnects) with a still-dirty handle
// whose close-flush fails must record the loss at mount scope, so a COMMIT for
// the same file arriving later on a PEER connection surfaces it instead of
// falsely succeeding via commitByPath. The per-connection cache that held the
// data is gone, so only a Server-scoped record can be observed.
func TestDrainFlushErrorSurfacedByPeerCommit(t *testing.T) {
	srv := &Server{}
	connA := &conn{Server: srv} // will receive the COMMIT
	connB := &conn{Server: srv} // holds the dirty handle, then drains
	srv.registerConn(connA)
	srv.registerConn(connB)

	key := "drain-handle16!"
	flushErr := errors.New("ENOSPC")

	// Connection B caches a dirty handle whose close fails, then disconnects.
	connB.writeHandleCache().put(key, &cachedHandle{
		file: closeErrFile{err: flushErr}, dirty: true, lastUsed: time.Now(),
	})
	connB.drainCaches()
	srv.unregisterConn(connB)

	// COMMIT arrives on connection A. flushHandleAll finds no live handle...
	found, ferr := srv.flushHandleAll([]byte(key))
	if found {
		t.Fatal("no live handle should remain after peer drained")
	}
	if ferr != nil {
		t.Fatalf("flushHandleAll: %v", ferr)
	}
	// ...but the mount-wide tracker must surface the drained connection's loss.
	if got := srv.commitsTracker().wait(key); got != flushErr {
		t.Fatalf("peer COMMIT got %v, want drained flush error %v", got, flushErr)
	}

	connA.drainCaches()
}

// P1: when the cache is full and an LRU eviction's close-flush fails, a COMMIT
// for the evicted file — even racing in the eviction window — must observe the
// failure. The tracker marks the eviction pending before the victim leaves the
// cache, so wait() blocks for the result rather than racing to commitByPath.
func TestEvictionWindowSurfacedByCommit(t *testing.T) {
	c := newWriteCache(nil)
	defer c.Close()
	c.maxSize = 1

	victimKey := "victim-handle16!"
	flushErr := errors.New("EIO")
	c.put(victimKey, &cachedHandle{file: closeErrFile{err: flushErr}, dirty: true, lastUsed: time.Now()})

	// Inserting a second key evicts the victim (maxSize 1), whose flush fails.
	c.put("next-handle16!!", &cachedHandle{file: closeErrFile{}, lastUsed: time.Now()})

	// A COMMIT for the victim must surface the loss.
	if got := c.takeCommitErr(victimKey); got != flushErr {
		t.Fatalf("COMMIT after eviction got %v, want %v", got, flushErr)
	}
	// Consumed once.
	if got := c.takeCommitErr(victimKey); got != nil {
		t.Fatalf("second COMMIT got %v, want nil", got)
	}
}

// A clean handle evicted on drain (close fails but nothing was dirty) must not
// record a commit error: there is no acknowledged unstable data to lose.
func TestDrainCleanHandleNoCommitError(t *testing.T) {
	srv := &Server{}
	c := &conn{Server: srv}
	srv.registerConn(c)

	key := "clean-drain16!!"
	c.writeHandleCache().put(key, &cachedHandle{
		file: closeErrFile{err: errors.New("EIO")}, dirty: false, lastUsed: time.Now(),
	})
	c.drainCaches()
	srv.unregisterConn(c)

	if got := srv.commitsTracker().wait(key); got != nil {
		t.Fatalf("clean drain must not record commit error, got %v", got)
	}
}

// End-to-end-ish: the tracker is shared across a server's connections, so a
// dirty handle dropped on one connection (handle-mutating op broadcast) with a
// failing close is visible to a COMMIT on another.
func TestDropHandleFlushErrorSurfacedByPeerCommit(t *testing.T) {
	srv := &Server{}
	connA := &conn{Server: srv}
	connB := &conn{Server: srv}
	srv.registerConn(connA)
	srv.registerConn(connB)
	defer func() {
		connA.drainCaches()
		connB.drainCaches()
	}()

	key := "dropped-handle1!"
	flushErr := errors.New("ENOSPC")
	connB.writeHandleCache().put(key, &cachedHandle{
		file: closeErrFile{err: flushErr}, dirty: true, lastUsed: time.Now(),
	})

	// Broadcast drop (e.g. SETATTR/REMOVE/RENAME) closes B's dirty handle.
	srv.dropHandleAll([]byte(key))

	if got := srv.commitsTracker().wait(key); got != flushErr {
		t.Fatalf("peer COMMIT after drop got %v, want %v", got, flushErr)
	}
}

// Sanity: a normal flush-on-COMMIT of a dirty handle (osfs, real Sync) leaves
// no tracker error and reports the data durable.
func TestCommitCleanPathNoTrackerError(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)
	name := "f.dat"
	if f, err := fs.Create(name); err != nil {
		t.Fatalf("create: %v", err)
	} else {
		_ = f.Close()
	}

	srv := &Server{}
	c := &conn{Server: srv}
	srv.registerConn(c)
	defer c.drainCaches()

	key := "ok-handle16!!!!"
	wf, err := fs.OpenFile(name, 0, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	c.writeHandleCache().put(key, &cachedHandle{file: &syncSpyFile{File: wf}, dirty: true, lastUsed: time.Now()})

	found, ferr := srv.flushHandleAll([]byte(key))
	if !found || ferr != nil {
		t.Fatalf("flushHandleAll found=%v err=%v, want true/nil", found, ferr)
	}
	if got := srv.commitsTracker().wait(key); got != nil {
		t.Fatalf("clean COMMIT got tracker error %v, want nil", got)
	}
}

// P1-b: on a backend without Sync, COMMIT flushes by closing the dirty handle.
// If that close fails, the handle is gone, so the failure must be recorded on
// the mount-wide tracker — otherwise a retried COMMIT would miss the (now
// absent) dirty handle and falsely succeed via commitByPath.
func TestFlushHandleNoSyncCloseErrorRecorded(t *testing.T) {
	srv := &Server{}
	c := &conn{Server: srv}
	srv.registerConn(c)
	defer c.drainCaches()

	key := "nosync-handle1!"
	flushErr := errors.New("ENOSPC")
	// closeErrFile exposes no Sync and fails to Close: the no-Sync fallback fires.
	c.writeHandleCache().put(key, &cachedHandle{
		file: closeErrFile{err: flushErr}, dirty: true, lastUsed: time.Now(),
	})

	found, err := c.flushHandle([]byte(key))
	if !found || err != flushErr {
		t.Fatalf("flushHandle found=%v err=%v, want true/%v", found, err, flushErr)
	}
	// The handle is gone; a retried COMMIT must still see the loss via the tracker.
	if got := srv.commitsTracker().wait(key); got != flushErr {
		t.Fatalf("retried COMMIT got %v, want recorded %v", got, flushErr)
	}
}

// P1-c: a dataSync/fileSync WRITE whose fd is closed by a concurrent drop must
// not assume durability. If that drop's close-flush failed (recorded on the
// tracker), the stable WRITE must surface the error rather than reply fileSync
// OK — its client will not send a COMMIT to learn of a deferred failure. This
// checks the tracker consultation the onWrite errHandleClosed branch relies on.
func TestStableWriteSurfacesConcurrentCloseError(t *testing.T) {
	srv := &Server{}
	c := &conn{Server: srv}
	srv.registerConn(c)
	defer c.drainCaches()

	key := "stable-handle1!"
	flushErr := errors.New("EIO")

	// Model the concurrent drop: a dirty handle dropped across connections whose
	// close fails records the loss on the tracker (same path onWrite waits on).
	c.writeHandleCache().put(key, &cachedHandle{
		file: closeErrFile{err: flushErr}, dirty: true, lastUsed: time.Now(),
	})
	srv.dropHandleAll([]byte(key))

	// The stable-write recovery path consults the tracker for this handle.
	if got := srv.commitsTracker().wait(key); got != flushErr {
		t.Fatalf("stable WRITE recovery got %v, want %v", got, flushErr)
	}
}

// P1 (READ-triggered flush): on a no-Sync backend, a READ flushes a dirty
// handle by closing it (flushDirty). If that Close fails, the handle is detached
// from the cache, so the loss must be recorded on the mount-wide tracker —
// otherwise a later/racing COMMIT for the same file finds no state and falsely
// succeeds via commitByPath.
func TestFlushDirtyNoSyncCloseErrorRecorded(t *testing.T) {
	srv := &Server{}
	c := &conn{Server: srv}
	srv.registerConn(c)
	defer c.drainCaches()

	key := "rdflush-handle1!"
	flushErr := errors.New("ENOSPC")
	c.writeHandleCache().put(key, &cachedHandle{
		file: closeErrFile{err: flushErr}, dirty: true, lastUsed: time.Now(),
	})

	// READ broadcast flushes dirty handles; the no-Sync fallback closes-and-drops.
	if err := srv.flushDirtyHandleAll([]byte(key)); err != flushErr {
		t.Fatalf("flushDirtyHandleAll got %v, want %v", err, flushErr)
	}
	// Handle is detached; a later COMMIT must still surface the loss via tracker.
	if got := srv.commitsTracker().wait(key); got != flushErr {
		t.Fatalf("COMMIT after READ-flush got %v, want recorded %v", got, flushErr)
	}
}

// A clean handle flushed-by-close on a no-Sync backend whose Close fails must
// NOT record a commit error: nothing unstable was pending. Exercises
// flushHandle's no-Sync path (flushDirty only runs for dirty handles).
func TestFlushHandleNoSyncCleanCloseNoError(t *testing.T) {
	srv := &Server{}
	c := &conn{Server: srv}
	srv.registerConn(c)
	defer c.drainCaches()

	key := "cleanflush-handl"
	c.writeHandleCache().put(key, &cachedHandle{
		file: closeErrFile{err: errors.New("EIO")}, dirty: false, lastUsed: time.Now(),
	})

	// flushHandle syncs (no Sync → close). Clean handle: close error is not a loss.
	_, _ = c.flushHandle([]byte(key))
	if got := srv.commitsTracker().wait(key); got != nil {
		t.Fatalf("clean no-Sync close must not record commit error, got %v", got)
	}
}

// P2 (READ races a failed concurrent dirty-close): if a dirty write handle is
// closed by a concurrent eviction/drop AND that close-flush failed (the loss
// recorded on the tracker), a READ flushing the same handle must surface the
// error rather than treat the closed handle as "nothing dirty" and serve stale
// data. flushDirty must consult the tracker for a closed handle.
func TestFlushDirtyClosedHandleSurfacesTrackerError(t *testing.T) {
	srv := &Server{}
	c := &conn{Server: srv}
	srv.registerConn(c)
	defer c.drainCaches()

	key := "rdrace-handle16!"
	lostErr := errors.New("ENOSPC")

	// A handle already closed by a concurrent path, whose close-flush lost data
	// (recorded on the tracker, as evictFlush/finish would).
	h := &cachedHandle{file: closeErrFile{}, dirty: true, lastUsed: time.Now()}
	_, _ = h.evictFlush() // marks closed
	c.writeHandleCache().put(key, h)
	srv.commitsTracker().record(key, lostErr)

	// The READ-triggered flush must surface the recorded loss, not ignore it.
	if err := srv.flushDirtyHandleAll([]byte(key)); err != lostErr {
		t.Fatalf("flushDirtyHandleAll got %v, want %v", err, lostErr)
	}
}

// P2 (READ miss races an in-flight/failed eviction): the dirty handle has already
// been removed from the cache (LRU/idle/drain/drop detach it before flushing), so
// flushDirty's cache lookup misses. On a no-Sync/buffered backend that eviction's
// close is what makes the writes durable, and it may have failed — so the READ
// miss path must consult the tracker rather than report "nothing dirty" and let
// onRead serve a fresh read-only fd's stale bytes.
func TestFlushDirtyMissSurfacesTrackerError(t *testing.T) {
	srv := &Server{}
	c := &conn{Server: srv}
	srv.registerConn(c)
	defer c.drainCaches()

	key := "rdmiss-handle16!"
	lostErr := errors.New("ENOSPC")

	// No handle is cached (already evicted), but its failed close-flush is recorded
	// on the mount-wide tracker, exactly as evictFlush/finish would leave it.
	_ = c.writeHandleCache() // create the (empty) cache
	srv.commitsTracker().record(key, lostErr)

	if err := srv.flushDirtyHandleAll([]byte(key)); err != lostErr {
		t.Fatalf("READ miss got %v, want recorded loss %v", err, lostErr)
	}
}

// P2 (READ after the recording connection drained): the connection that cached
// the dirty handle and recorded its failed close has since disconnected
// (drainCaches + unregisterConn), so no live connection has a write cache for the
// handle and none of the per-connection flushDirty calls reach the tracker.
// flushDirtyHandleAll must still await the mount-wide tracker so the READ surfaces
// the loss instead of serving stale bytes — and must not consume it, so the
// client's COMMIT still sees it.
func TestFlushDirtyHandleAllAwaitsTrackerAfterPeerDrained(t *testing.T) {
	srv := &Server{}
	connA := &conn{Server: srv} // serves the READ, never cached this handle
	connB := &conn{Server: srv} // recorded the loss, then drains
	srv.registerConn(connA)
	srv.registerConn(connB)

	key := "drained-rd16!!!!"
	lostErr := errors.New("ENOSPC")

	// Connection B caches a dirty handle whose close fails, then disconnects: the
	// loss lands on the mount-wide tracker and B leaves the live set.
	connB.writeHandleCache().put(key, &cachedHandle{
		file: closeErrFile{err: lostErr}, dirty: true, lastUsed: time.Now(),
	})
	connB.drainCaches()
	srv.unregisterConn(connB)

	// A READ on connection A (no write cache for the handle) must still see the loss.
	if err := srv.flushDirtyHandleAll([]byte(key)); err != lostErr {
		t.Fatalf("READ after peer drain got %v, want %v", err, lostErr)
	}
	// Non-consuming: the client's COMMIT must still surface it.
	if got := srv.commitsTracker().wait(key); got != lostErr {
		t.Fatalf("COMMIT after READ got %v, want %v (READ must not consume)", got, lostErr)
	}

	connA.drainCaches()
}
func TestReadFlushDoesNotConsumeCommitError(t *testing.T) {
	srv := &Server{}
	c := &conn{Server: srv}
	srv.registerConn(c)
	defer c.drainCaches()

	key := "rdkeep-handle16!"
	lostErr := errors.New("EIO")
	_ = c.writeHandleCache()
	srv.commitsTracker().record(key, lostErr)

	// READ surfaces the loss...
	if err := srv.flushDirtyHandleAll([]byte(key)); err != lostErr {
		t.Fatalf("READ flush got %v, want %v", err, lostErr)
	}
	// ...and the subsequent COMMIT must STILL see it (READ did not consume).
	if got := srv.commitsTracker().wait(key); got != lostErr {
		t.Fatalf("COMMIT after READ got %v, want %v (READ must not consume)", got, lostErr)
	}
	// COMMIT consumes it.
	if got := srv.commitsTracker().wait(key); got != nil {
		t.Fatalf("after COMMIT got %v, want nil", got)
	}
}
