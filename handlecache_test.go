package nfs

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
)

// osfs files implement Sync via *os.File, so this exercises the real
// flush-without-close COMMIT path.
func TestWriteCacheReusesHandle(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)

	name := "f.dat"
	if f, err := fs.Create(name); err != nil {
		t.Fatalf("create: %v", err)
	} else {
		_ = f.Close()
	}

	c := newWriteCache(nil)
	defer c.Close()

	key := "handle-1"

	// First write opens and caches a handle.
	h := c.get(key)
	if h != nil {
		t.Fatal("expected empty cache")
	}
	file, err := fs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	h = &cachedHandle{file: file, lastUsed: time.Now()}
	c.put(key, h)

	if _, err := h.writeAt([]byte("hello"), 0); err != nil {
		t.Fatalf("writeAt: %v", err)
	}
	if !h.dirtyState() {
		t.Fatal("expected handle to be dirty after write")
	}

	// Second write reuses the same cached handle (no new open).
	if got := c.get(key); got != h {
		t.Fatal("expected cached handle reuse")
	}
	if _, err := h.writeAt([]byte(" world"), 5); err != nil {
		t.Fatalf("writeAt 2: %v", err)
	}

	// COMMIT: sync() flushes if supported; osfs's billy.File does not expose
	// Sync, so it reports unsupported and the caller flushes via closeFlush.
	synced, err := h.sync()
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if !synced {
		c.remove(key)
		if err := h.closeFlush(); err != nil {
			t.Fatalf("closeFlush: %v", err)
		}
	}

	// Data is durable on disk either way.
	got, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("readfile: %v", err)
	}
	if string(got) != "hello world" {
		t.Fatalf("got %q, want %q", got, "hello world")
	}
}

// syncableFile wraps an osfs file to expose Sync, mirroring the e2b chroot
// wrappedFile, so the flush-without-close COMMIT path is covered too.
type syncableFile struct {
	billy.File
	f *os.File
}

func (s syncableFile) Sync() error { return s.f.Sync() }

func TestCachedHandleSyncFlushesWithoutClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "g.dat")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()

	h := &cachedHandle{file: syncableFile{File: nil, f: f}, lastUsed: time.Now()}
	// write directly via the os.File since the embedded billy.File is nil in
	// this minimal wrapper; we only exercise sync().
	if _, err := f.WriteString("data"); err != nil {
		t.Fatalf("write: %v", err)
	}
	h.markDirty(4)

	synced, err := h.sync()
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if !synced {
		t.Fatal("expected Sync to be supported")
	}
	if h.dirtyState() {
		t.Fatal("expected clean after sync")
	}
}

type blockingWriteAtFile struct {
	billy.File
	entered chan struct{}
	release chan struct{}
}

func (f *blockingWriteAtFile) WriteAt(p []byte, off int64) (int, error) {
	f.entered <- struct{}{}
	<-f.release

	return len(p), nil
}

func TestCachedHandleWriteAtDoesNotSerializePositionalWrites(t *testing.T) {
	f := &blockingWriteAtFile{
		entered: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	h := &cachedHandle{file: f, lastUsed: time.Now()}

	done := make(chan error, 2)
	go func() {
		_, err := h.writeAt([]byte("first"), 0)
		done <- err
	}()

	select {
	case <-f.entered:
	case <-time.After(time.Second):
		t.Fatal("first write did not start")
	}

	go func() {
		_, err := h.writeAt([]byte("second"), 4096)
		done <- err
	}()

	select {
	case <-f.entered:
	case <-time.After(time.Second):
		t.Fatal("second WriteAt did not start while first write was in flight")
	}

	close(f.release)
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
}

type blockingSeekWriteFile struct {
	billy.File
	entered chan struct{}
	release chan struct{}
}

func (f *blockingSeekWriteFile) Seek(int64, int) (int64, error) { return 0, nil }

func (f *blockingSeekWriteFile) Write(p []byte) (int, error) {
	f.entered <- struct{}{}
	<-f.release

	return len(p), nil
}

func TestCachedHandleSeekWriteFallbackSerializesOffsetWrites(t *testing.T) {
	f := &blockingSeekWriteFile{
		entered: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	h := &cachedHandle{file: f, lastUsed: time.Now()}

	done := make(chan error, 2)
	go func() {
		_, err := h.writeAt([]byte("first"), 0)
		done <- err
	}()

	select {
	case <-f.entered:
	case <-time.After(time.Second):
		t.Fatal("first fallback write did not start")
	}

	go func() {
		_, err := h.writeAt([]byte("second"), 4096)
		done <- err
	}()

	select {
	case <-f.entered:
		t.Fatal("fallback Seek+Write path allowed concurrent offset writes")
	case <-time.After(50 * time.Millisecond):
	}

	close(f.release)
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil && err != io.EOF {
			t.Fatalf("fallback write %d: %v", i, err)
		}
	}
}

func TestWriteCacheEvictsOldest(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)

	c := newWriteCache(nil)
	defer c.Close()
	c.maxSize = 2

	open := func(name string) *cachedHandle {
		if f, err := fs.Create(name); err == nil {
			_ = f.Close()
		}
		file, err := fs.OpenFile(name, os.O_RDWR, 0o644)
		if err != nil {
			t.Fatalf("open %s: %v", name, err)
		}

		return &cachedHandle{file: file, lastUsed: time.Now()}
	}

	c.put("a", open("a"))
	time.Sleep(2 * time.Millisecond)
	c.put("b", open("b"))
	time.Sleep(2 * time.Millisecond)
	c.put("c", open("c")) // should evict "a" (oldest)

	if c.get("a") != nil {
		t.Fatal("expected oldest handle 'a' to be evicted")
	}
	if c.get("b") == nil || c.get("c") == nil {
		t.Fatal("expected 'b' and 'c' to remain cached")
	}
}

func TestCachedHandleRejectsWriteAfterClose(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)
	if f, err := fs.Create("x"); err == nil {
		_ = f.Close()
	}
	file, err := fs.OpenFile("x", os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	h := &cachedHandle{file: file, lastUsed: time.Now()}

	if err := h.closeFlush(); err != nil {
		t.Fatalf("closeFlush: %v", err)
	}
	if _, err := h.writeAt([]byte("y"), 0); err == nil {
		t.Fatal("expected write after close to fail")
	}
}

// TestPostOpInfoTracksSizeWithoutFstat verifies the post-op attribute fast path:
// after a first real fstat (stat) caches the immutable fields, postOpInfo
// reports a growing size from local write tracking (no fstat) while preserving
// the file mode, and reflects later writes that extend the file.
func TestPostOpInfoTracksSizeWithoutFstat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "grow.dat")
	osf, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer osf.Close()

	// statWriteFile exposes Stat + WriteAt (like the e2b chroot wrappedFile,
	// unlike stock osfs whose billy.File has neither), so the handle takes the
	// fstat-caching and WriteAt paths under test.
	h := &cachedHandle{file: statWriteFile{f: osf}, lastUsed: time.Now()}

	// Before any stat, postOpInfo must miss (forces a real stat first).
	if _, ok := h.postOpInfo(); ok {
		t.Fatal("postOpInfo should miss before first stat")
	}

	// Write 5 bytes at offset 0, then a real stat to seed the cache.
	if _, err := h.writeAt([]byte("hello"), 0); err != nil {
		t.Fatalf("writeAt: %v", err)
	}
	fi, ok := h.stat()
	if !ok {
		t.Fatal("stat should succeed on osfs file")
	}
	if fi.Size() != 5 {
		t.Fatalf("stat size = %d, want 5", fi.Size())
	}

	// Extend the file with a write at offset 5; postOpInfo must now report 10
	// without an fstat, preserving the mode.
	if _, err := h.writeAt([]byte("world"), 5); err != nil {
		t.Fatalf("writeAt 2: %v", err)
	}
	pi, ok := h.postOpInfo()
	if !ok {
		t.Fatal("postOpInfo should hit after stat")
	}
	if pi.Size() != 10 {
		t.Fatalf("postOpInfo size = %d, want 10", pi.Size())
	}
	if pi.Mode() != fi.Mode() {
		t.Fatalf("postOpInfo mode = %v, want %v (immutable field must be preserved)", pi.Mode(), fi.Mode())
	}

	// A write that does not extend the file (overwrite within bounds) must not
	// shrink the reported size.
	if _, err := h.writeAt([]byte("HE"), 0); err != nil {
		t.Fatalf("writeAt 3: %v", err)
	}
	pi, _ = h.postOpInfo()
	if pi.Size() != 10 {
		t.Fatalf("postOpInfo size after in-bounds write = %d, want 10", pi.Size())
	}
}

// statWriteFile is a billy.File backed by an *os.File that exposes Stat and
// WriteAt (the capabilities the e2b chroot wrappedFile provides and stock osfs
// does not), for exercising the cachedHandle fstat-cache and WriteAt paths.
type statWriteFile struct {
	billy.File
	f *os.File
}

func (s statWriteFile) Stat() (os.FileInfo, error)              { return s.f.Stat() }
func (s statWriteFile) WriteAt(p []byte, off int64) (int, error) { return s.f.WriteAt(p, off) }
func (s statWriteFile) Close() error                           { return s.f.Close() }

// TestPostOpInfoRaceWithClose exercises concurrent postOpInfo reads against a
// close/evict, which writes h.closed under h.mu while postOpInfo reads it
// lock-free. Run with -race: before closed was made an atomic.Bool this raced
// (postOpInfo read under stateMu, close wrote under h.mu).
func TestPostOpInfoRaceWithClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "race.dat")
	osf, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	h := &cachedHandle{file: statWriteFile{f: osf}, lastUsed: time.Now()}
	// Seed the immutable-attr cache so postOpInfo can hit.
	if _, ok := h.stat(); !ok {
		t.Fatal("stat should succeed")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			_, _ = h.postOpInfo()
		}
	}()
	// Concurrently evict/close the handle (writes h.closed under h.mu).
	_, _ = h.evictFlush()
	<-done
}
