package nfs

import (
	"os"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5/osfs"
)

// TestMaxReadMatchesAdvertisedRtmax guards against MaxRead drifting above the
// rtmax advertised in onFSInfo. onRead allocates a buffer of the clamped READ
// count, and with concurrent per-connection workers many such buffers can be
// live at once; a MaxRead larger than the advertised limit would let a client
// force oversized allocations on every worker (the bound onFSInfo promises).
// They must stay equal — onFSInfo advertises MaxRead directly, this pins the
// value so it is not raised in isolation.
func TestMaxReadMatchesAdvertisedRtmax(t *testing.T) {
	const advertisedRtmax = 1 << 20
	if MaxRead != advertisedRtmax {
		t.Fatalf("MaxRead = %d, want %d (must match advertised rtmax so concurrent "+
			"READ workers cannot allocate above the advertised per-request bound)",
			MaxRead, advertisedRtmax)
	}
}

func TestReadCacheReusesHandle(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)

	name := "r.dat"
	f, err := fs.Create(name)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := f.Write([]byte("hello world")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = f.Close()

	c := newReadCache()
	defer c.Close()

	key := "rh-1"
	if c.get(key) != nil {
		t.Fatal("expected empty cache")
	}

	fh, err := fs.Open(name)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	h := &readHandle{file: fh}
	h.lastUsed.Store(time.Now().UnixNano())
	c.put(key, h)

	buf := make([]byte, 5)
	if _, err := h.readAt(buf, 0); err != nil {
		t.Fatalf("readAt: %v", err)
	}
	if string(buf) != "hello" {
		t.Fatalf("got %q", buf)
	}

	// Reused on second lookup.
	if c.get(key) != h {
		t.Fatal("expected cached read handle reuse")
	}

	// fstat via the handle if the backing file supports it. osfs's billy.File
	// does not expose Stat (the e2b wrappedFile does), so only assert when
	// available; onRead falls back to a path stat otherwise.
	if fi, ok := h.stat(); ok && fi.Size() != 11 {
		t.Fatalf("stat size: got %d want 11", fi.Size())
	}
}

func TestReadCacheEvictsOldest(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)
	c := newReadCache()
	defer c.Close()
	c.maxSize = 2

	open := func(name string) *readHandle {
		f, err := fs.Create(name)
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		_ = f.Close()
		fh, err := fs.Open(name)
		if err != nil {
			t.Fatalf("open %s: %v", name, err)
		}
		h := &readHandle{file: fh}
		h.lastUsed.Store(time.Now().UnixNano())

		return h
	}

	c.put("a", open("a"))
	time.Sleep(2 * time.Millisecond)
	c.put("b", open("b"))
	time.Sleep(2 * time.Millisecond)
	c.put("c", open("c"))

	if c.get("a") != nil {
		t.Fatal("expected oldest read handle 'a' evicted")
	}
	if c.get("b") == nil || c.get("c") == nil {
		t.Fatal("expected 'b' and 'c' to remain")
	}
}

func TestReadHandleRejectsAfterClose(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)
	f, err := fs.Create("x")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = f.Close()
	fh, err := fs.Open("x")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	h := &readHandle{file: fh}
	if err := h.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := h.readAt(make([]byte, 1), 0); err == nil {
		t.Fatal("expected read after close to fail")
	}
	_ = os.Remove(dir + "/x")
}
