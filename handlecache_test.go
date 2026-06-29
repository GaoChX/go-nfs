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
	h.markDirty()

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
