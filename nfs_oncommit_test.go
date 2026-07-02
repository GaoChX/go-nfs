package nfs

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
)

// noSyncFS wraps a filesystem so its files do NOT expose Sync(), modelling a
// backend that buffers writes inside the fd and only flushes on Close. Used to
// exercise the commitByPath fallback when no inode-level fsync is available.
type noSyncFS struct {
	billy.Filesystem
}

func (f noSyncFS) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	file, err := f.Filesystem.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	return noSyncFile{file}, nil
}

// noSyncFile hides any Sync method the embedded file might have by only
// promoting the billy.File interface, which has no Sync.
type noSyncFile struct {
	billy.File
}

// syncableOSFS wraps osfs so its files expose Sync(), modelling the real e2b
// chroot backend whose wrappedFile forwards Sync. (Plain osfs returns a
// chroot.file that embeds the billy.File interface and does NOT promote Sync.)
type syncableOSFS struct {
	billy.Filesystem
}

func (f syncableOSFS) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	file, err := f.Filesystem.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	// osfs files embed *os.File deep down; reach a real fd to fsync.
	return osSyncFile{file, filepath.Join(f.Root(), name)}, nil
}

type osSyncFile struct {
	billy.File
	realPath string
}

func (s osSyncFile) Sync() error {
	fd, err := os.OpenFile(s.realPath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer fd.Close()

	return fd.Sync()
}

// commitByPath must succeed against a Sync-capable backend (e.g. e2b): fsync is
// inode-level, so it flushes writes made on another connection's fd.
func TestCommitByPathSyncs(t *testing.T) {
	dir := t.TempDir()
	fs := syncableOSFS{osfs.New(dir)}

	name := "f.dat"
	f, err := fs.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := f.Write([]byte("data")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = f.Close()

	if err := commitByPath(fs, fs.Join(name)); err != nil {
		t.Fatalf("commitByPath on syncable backend: %v", err)
	}
}

// When the backend cannot Sync, commitByPath falls back to open+close as a best
// effort and must still report success: the file may already be clean (its dirty
// handle was evicted and flushed, or the COMMIT has nothing pending). Failing
// here would turn ordinary valid COMMITs into NFS I/O errors.
func TestCommitByPathNoSyncSucceeds(t *testing.T) {
	dir := t.TempDir()
	fs := noSyncFS{osfs.New(dir)}

	name := "f.dat"
	f, err := fs.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_ = f.Close()

	if err := commitByPath(fs, fs.Join(name)); err != nil {
		t.Fatalf("commitByPath on a clean file without Sync should succeed, got %v", err)
	}
}

// A missing file is nothing to commit and must not error.
func TestCommitByPathMissingFileOK(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)

	if err := commitByPath(fs, fs.Join("does-not-exist.dat")); err != nil {
		t.Fatalf("commitByPath on missing file should be nil, got %v", err)
	}
	// sanity: the path really is absent.
	if _, err := os.Stat(filepath.Join(dir, "does-not-exist.dat")); !os.IsNotExist(err) {
		t.Fatalf("expected file to be absent")
	}
}

// syncSpyFile is a billy.File that records whether Sync was called on it,
// modelling a backend that buffers writes inside the fd and whose Sync is the
// only way to make them durable. Used to prove COMMIT flushes a dirty handle
// held by a *different* connection (nconnect).
type syncSpyFile struct {
	billy.File
	mu     sync.Mutex
	synced bool
}

func (s *syncSpyFile) Sync() error {
	s.mu.Lock()
	s.synced = true
	s.mu.Unlock()

	return nil
}

func (s *syncSpyFile) didSync() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.synced
}

// With nconnect, the unstable writes for a file can be cached on a different
// connection than the one that receives COMMIT. flushHandleAll must reach the
// peer connection's dirty handle and Sync it; flushing only the local cache
// would acknowledge data still buffered in the peer's fd on backends whose Sync
// is per-fd (not inode-wide).
func TestFlushHandleAllFlushesPeerConnection(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)

	name := "f.dat"
	if f, err := fs.Create(name); err != nil {
		t.Fatalf("create: %v", err)
	} else {
		_ = f.Close()
	}

	srv := &Server{}
	connA := &conn{Server: srv} // receives COMMIT, holds no cached handle
	connB := &conn{Server: srv} // holds the dirty cached write handle
	srv.registerConn(connA)
	srv.registerConn(connB)
	defer func() {
		connA.drainCaches()
		connB.drainCaches()
	}()

	key := "shared-handle16!"

	// Seed a dirty, Sync-capable handle on connection B only.
	bf, err := fs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	spy := &syncSpyFile{File: bf}
	connB.writeHandleCache().put(key, &cachedHandle{file: spy, dirty: true, lastUsed: time.Now()})

	// COMMIT arrives on connection A, which has no cached handle for the file.
	found, ferr := srv.flushHandleAll([]byte(key))
	if ferr != nil {
		t.Fatalf("flushHandleAll: %v", ferr)
	}
	if !found {
		t.Fatal("expected flushHandleAll to find the peer connection's cached handle")
	}
	if !spy.didSync() {
		t.Fatal("peer connection's dirty handle was not synced by COMMIT on another connection")
	}
}

// When no connection holds a cached handle, flushHandleAll reports not-found so
// onCommit falls back to commitByPath.
func TestFlushHandleAllNoCachedHandle(t *testing.T) {
	srv := &Server{}
	c := &conn{Server: srv}
	srv.registerConn(c)
	_ = c.writeHandleCache() // exists but empty
	defer c.drainCaches()

	found, err := srv.flushHandleAll([]byte("never-written16!"))
	if err != nil {
		t.Fatalf("flushHandleAll: %v", err)
	}
	if found {
		t.Fatal("expected no cached handle to be found")
	}
}

// closeErrFile is a billy.File whose Close fails, modelling an ENOSPC/EIO on the
// final flush of buffered unstable writes.
type closeErrFile struct {
	billy.File
	err error
}

func (f closeErrFile) Close() error { return f.err }

// Evicting a still-dirty handle whose close-flush fails must not silently drop
// the error: it is stashed and surfaced by the next COMMIT for that file, so a
// client is told its unstable writes were not made durable instead of a false
// success via commitByPath.
func TestEvictDirtyFlushErrorSurfacedByCommit(t *testing.T) {
	c := newWriteCache(nil)
	defer c.Close()
	c.maxSize = 1

	key := "dirty-handle16!!"
	flushErr := errors.New("ENOSPC")
	// A dirty handle backed by a file that fails to close.
	c.put(key, &cachedHandle{file: closeErrFile{err: flushErr}, dirty: true, lastUsed: time.Now()})

	// Cache full (maxSize 1); inserting another key evicts the dirty handle,
	// whose close-flush fails. The error must be recorded against its key.
	c.put("other-handle16!", &cachedHandle{file: closeErrFile{}, lastUsed: time.Now()})

	if got := c.takeCommitErr(key); got != flushErr {
		t.Fatalf("expected eviction flush error %v surfaced, got %v", flushErr, got)
	}
	// takeCommitErr clears it; a second COMMIT sees nothing.
	if got := c.takeCommitErr(key); got != nil {
		t.Fatalf("expected commit error cleared after take, got %v", got)
	}
}

// A clean handle (no pending writes) whose close fails must NOT record a commit
// error: there is nothing unflushed to lose.
func TestEvictCleanFlushErrorNotRecorded(t *testing.T) {
	c := newWriteCache(nil)
	defer c.Close()
	c.maxSize = 1

	key := "clean-handle16!!"
	c.put(key, &cachedHandle{file: closeErrFile{err: errors.New("EIO")}, dirty: false, lastUsed: time.Now()})
	c.put("other-handle16!", &cachedHandle{file: closeErrFile{}, lastUsed: time.Now()})

	if got := c.takeCommitErr(key); got != nil {
		t.Fatalf("clean eviction must not record a commit error, got %v", got)
	}
}

// A pending eviction flush error must survive a subsequent write to the same
// key: re-caching a handle does not recover the unstable bytes already lost on
// the failed close, so only a COMMIT (takeCommitErr) may clear it. Otherwise a
// write after a lost-data eviction would let the next COMMIT falsely succeed.
func TestPutKeepsPendingCommitErr(t *testing.T) {
	c := newWriteCache(nil)
	defer c.Close()

	key := "reused-handle16!"
	flushErr := errors.New("old failure")
	c.commits.record(key, flushErr)
	c.put(key, &cachedHandle{file: closeErrFile{}, lastUsed: time.Now()})

	if got := c.takeCommitErr(key); got != flushErr {
		t.Fatalf("expected pending commit error preserved across put, got %v", got)
	}
}

// commitByPath must succeed on a read-only file: there is no cached dirty handle
// to flush, and a COMMIT of a file with nothing pending was a no-op success
// before the handle cache. Opening O_RDWR (or failing on EACCES) would regress
// that for read-only files or files whose write bit was chmod'd away.
func TestCommitByPathReadOnlyFileSucceeds(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)

	name := "ro.dat"
	f, err := fs.Create(name)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = f.Close()
	if err := os.Chmod(filepath.Join(dir, name), 0o444); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	if err := commitByPath(fs, fs.Join(name)); err != nil {
		t.Fatalf("commitByPath on read-only file should succeed, got %v", err)
	}
}
