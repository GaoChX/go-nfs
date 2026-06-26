package nfs

import (
	"os"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5/osfs"
)

// Read-after-unstable-write coherence: a prior unstable WRITE leaves dirty bytes
// only in the cached write fd. On backends whose billy.File buffers until
// Sync/Close, a READ served through a separate read-only fd would miss them, so
// onRead flushes any dirty write handle first. flushDirtyHandleAll must Sync a
// dirty handle — including one cached on a peer connection (nconnect).
func TestFlushDirtyHandleAllSyncsPeerConnection(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)

	name := "f.dat"
	if f, err := fs.Create(name); err != nil {
		t.Fatalf("create: %v", err)
	} else {
		_ = f.Close()
	}

	srv := &Server{}
	connA := &conn{Server: srv} // serves the READ, holds no cached write handle
	connB := &conn{Server: srv} // holds the dirty cached write handle
	srv.registerConn(connA)
	srv.registerConn(connB)
	defer func() {
		connA.drainCaches()
		connB.drainCaches()
	}()

	key := "shared-handle16!"

	bf, err := fs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	spy := &syncSpyFile{File: bf}
	connB.writeHandleCache().put(key, &cachedHandle{file: spy, dirty: true, lastUsed: time.Now()})

	// READ arrives on connection A, which has no cached handle for the file.
	if ferr := srv.flushDirtyHandleAll([]byte(key)); ferr != nil {
		t.Fatalf("flushDirtyHandleAll: %v", ferr)
	}
	if !spy.didSync() {
		t.Fatal("dirty write handle on peer connection was not synced before READ")
	}
}

// A clean (non-dirty) cached write handle must NOT be synced on every READ:
// read-only and post-COMMIT workloads should pay only a cache lookup.
func TestFlushDirtyHandleAllSkipsCleanHandle(t *testing.T) {
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

	key := "clean-handle16!!"
	bf, err := fs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	spy := &syncSpyFile{File: bf}
	// dirty:false — nothing pending.
	c.writeHandleCache().put(key, &cachedHandle{file: spy, dirty: false, lastUsed: time.Now()})

	if ferr := srv.flushDirtyHandleAll([]byte(key)); ferr != nil {
		t.Fatalf("flushDirtyHandleAll: %v", ferr)
	}
	if spy.didSync() {
		t.Fatal("clean handle should not be synced on READ")
	}
}

// syncIfDirty clears the dirty flag after a successful sync, so a clean handle
// is then skipped (no redundant Sync on subsequent reads with no new writes).
func TestSyncIfDirtyClearsDirty(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)
	name := "f.dat"
	if f, err := fs.Create(name); err != nil {
		t.Fatalf("create: %v", err)
	} else {
		_ = f.Close()
	}
	bf, err := fs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	h := &cachedHandle{file: &syncSpyFile{File: bf}, dirty: true, lastUsed: time.Now()}
	defer h.closeFlush()

	dirty, synced, closed, err := h.syncIfDirty()
	if err != nil || !dirty || !synced || closed {
		t.Fatalf("first syncIfDirty: dirty=%v synced=%v closed=%v err=%v, want true/true/false/nil", dirty, synced, closed, err)
	}
	// Now clean: a second call reports nothing to flush.
	dirty, _, _, err = h.syncIfDirty()
	if err != nil || dirty {
		t.Fatalf("second syncIfDirty: dirty=%v err=%v, want false/nil", dirty, err)
	}
}
