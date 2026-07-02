package nfs

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5/osfs"
)

// TestOnWriteInvalidatesReadCache verifies that a successful WRITE invalidates
// any cached read-only fd for the same handle (across the connection), so a
// later READ reopens instead of reusing a read fd that predates the write. On
// snapshot/buffered backends the stale read fd would return pre-write bytes;
// dropping the read cache on write is what prevents that. The write handle must
// be left intact so a run of unstable writes keeps reusing one fd.
func TestOnWriteInvalidatesReadCache(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)
	if f, err := fs.Create("data.bin"); err != nil {
		t.Fatalf("create: %v", err)
	} else {
		_ = f.Close()
	}

	handle := []byte("write-handle-rcinv")
	handler := &stubHandler{fs: fs, handle: handle, path: []string{"data.bin"}}
	srv := &Server{Handler: handler}
	c := &conn{Server: srv}
	srv.registerConn(c)
	defer c.drainCaches()

	// Seed a cached read-only fd for this handle, as a prior READ would have.
	rf, err := fs.OpenFile("data.bin", os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("open read fd: %v", err)
	}
	rh := &readHandle{file: rf}
	rh.lastUsed.Store(time.Now().UnixNano())
	c.readHandleCache().put(string(handle), rh)

	if c.readHandleCache().get(string(handle)) == nil {
		t.Fatal("precondition: read handle should be cached")
	}

	// A successful WRITE must invalidate the cached read fd.
	w := &response{
		conn:   c,
		writer: bytes.NewBuffer(nil),
		req:    &request{Body: buildWriteBody(t, handle, 0, []byte("hello"))},
	}
	if err := onWrite(context.Background(), w, handler); err != nil {
		t.Fatalf("onWrite: %v", err)
	}

	if c.readHandleCache().get(string(handle)) != nil {
		t.Fatal("read handle was not invalidated after WRITE; a later READ could serve stale bytes")
	}
	// The write handle must still be cached (write fd reuse preserved).
	if c.writeHandleCache().get(string(handle)) == nil {
		t.Fatal("write handle should remain cached after WRITE")
	}
}
