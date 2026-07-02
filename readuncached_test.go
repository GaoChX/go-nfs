package nfs

import (
	"bytes"
	"context"
	"testing"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// genBumpFS wraps a filesystem so every Open bumps the *same key's* read-cache
// invalidation generation as a side effect, modelling a concurrent
// SETATTR/REMOVE/etc. dropping the handle while a READ is mid-open. That makes
// readHandleFor's putIfFresh refuse the fd (opened under now-stale attributes).
type genBumpFS struct {
	billy.Filesystem
	rc  *readCache
	key string // the key whose gen to bump (same as the key being opened)
}

func (fs genBumpFS) Open(name string) (billy.File, error) {
	f, err := fs.Filesystem.Open(name)
	// Bump this key's gen AFTER readHandleFor sampled it but before putIfFresh
	// runs, so the publish is refused — modelling a concurrent SETATTR/etc. that
	// invalidated the handle while the open was in flight.
	fs.rc.invalidate(fs.key)
	return f, err
}

// TestReadHandleForServesUncachedWhenPublishRefused verifies that when caching a
// read fd is refused by a concurrent invalidation of the same handle (a racing
// SETATTR/REMOVE/etc.), readHandleFor still returns a valid handle to serve the
// read instead of failing with NFSStatusStale. The fd is fresh (just opened), so
// serving this one read from it is safe; we just decline to cache it for later
// reads (which would serve stale post-invalidation data).
func TestReadHandleForServesUncachedWhenPublishRefused(t *testing.T) {
	dir := t.TempDir()
	base := osfs.New(dir)
	if f, err := base.Create("data.bin"); err != nil {
		t.Fatalf("create: %v", err)
	} else {
		_ = f.Close()
	}

	handle := []byte("read-handle-uncached")
	handler := &stubHandler{fs: base, handle: handle, path: []string{"data.bin"}}
	srv := &Server{Handler: handler}
	c := &conn{Server: srv}
	srv.registerConn(c)
	defer c.drainCaches()

	key := string(handle)
	fs := genBumpFS{Filesystem: base, rc: c.readHandleCache(), key: key}

	h, cached, err := readHandleFor(&response{conn: c}, fs, handle, "data.bin")
	if err != nil {
		t.Fatalf("readHandleFor returned error (spurious stale?): %v", err)
	}
	if h == nil {
		t.Fatal("expected a usable handle to serve the read")
	}
	if cached {
		t.Fatal("expected an uncached handle (publish was refused every attempt)")
	}
	// The uncached handle must still be readable, and the caller owns closing it.
	if _, rerr := h.readAt(make([]byte, 0), 0); rerr != nil && rerr.Error() == "cached write handle closed" {
		t.Fatalf("handle not usable: %v", rerr)
	}
	if cerr := h.close(); cerr != nil {
		t.Fatalf("close uncached handle: %v", cerr)
	}
}

// TestOnReadSucceedsUnderUnrelatedInvalidation is the core per-key-gen guard: an
// unrelated file's invalidation (bumping only its own key's gen) must NOT refuse
// this READ's publish or turn it into a spurious ESTALE. It drives the full
// onRead path while every Open bumps a DIFFERENT key's gen, and asserts the READ
// succeeds and the handle is cached (proving the open was not falsely refused).
func TestOnReadSucceedsUnderUnrelatedInvalidation(t *testing.T) {
	dir := t.TempDir()
	base := osfs.New(dir)
	f, err := base.Create("data.bin")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := f.Write([]byte("hello world")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = f.Close()

	handle := []byte("read-handle-onread01")
	handler := &stubHandler{fs: base, handle: handle, path: []string{"data.bin"}}
	srv := &Server{Handler: handler}
	c := &conn{Server: srv}
	srv.registerConn(c)
	defer c.drainCaches()

	// Bump an UNRELATED key's gen on every open. With per-key gen this must not
	// affect this handle's publish.
	fs := genBumpFS{Filesystem: base, rc: c.readHandleCache(), key: "some-other-file"}
	handler.fs = fs

	w := &response{
		conn:   c,
		writer: bytes.NewBuffer(nil),
		req:    &request{Body: buildReadBody(t, handle, 0, 1024)},
	}
	if err := onRead(context.Background(), w, handler); err != nil {
		t.Fatalf("onRead returned error (spurious stale?): %v", err)
	}
	// The handle must have been cached (not refused by the unrelated invalidation).
	if c.readHandleCache().get(string(handle)) == nil {
		t.Fatal("read handle should be cached; an unrelated key's invalidation must not refuse this open")
	}
}

// buildReadBody builds an onRead request body: opaque handle, offset (u64),
// count (u32).
func buildReadBody(t *testing.T, handle []byte, offset uint64, count uint32) *bytes.Buffer {
	t.Helper()
	body := bytes.NewBuffer(nil)
	if err := xdr.Write(body, handle); err != nil {
		t.Fatalf("write handle: %v", err)
	}
	if err := xdr.Write(body, offset); err != nil {
		t.Fatalf("write offset: %v", err)
	}
	if err := xdr.Write(body, count); err != nil {
		t.Fatalf("write count: %v", err)
	}

	return body
}
