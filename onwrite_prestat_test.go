package nfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// buildWriteBody builds an onWrite request body: opaque handle, offset (u64),
// count (u32), how (u32, 0=unstable), then the data as opaque<>.
func buildWriteBody(t *testing.T, handle []byte, offset uint64, data []byte) *bytes.Buffer {
	t.Helper()
	body := bytes.NewBuffer(nil)
	if err := xdr.Write(body, handle); err != nil {
		t.Fatalf("write handle: %v", err)
	}
	if err := xdr.Write(body, offset); err != nil {
		t.Fatalf("write offset: %v", err)
	}
	if err := xdr.Write(body, uint32(len(data))); err != nil {
		t.Fatalf("write count: %v", err)
	}
	if err := xdr.Write(body, uint32(unstable)); err != nil {
		t.Fatalf("write how: %v", err)
	}
	if err := xdr.Write(body, data); err != nil {
		t.Fatalf("write data: %v", err)
	}

	return body
}

// TestOnWriteSkipsPreOpStatHappyPath verifies a normal unstable WRITE to an
// existing regular file still succeeds after the pre-op path Stat was removed
// (the file is validated by the O_RDWR open and the post-op fd fstat).
func TestOnWriteSkipsPreOpStatHappyPath(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)
	f, err := fs.Create("data.bin")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = f.Close()

	handle := []byte("write-handle-0001")
	handler := &stubHandler{fs: fs, handle: handle, path: []string{"data.bin"}}
	srv := &Server{Handler: handler}
	c := &conn{Server: srv}
	srv.registerConn(c)

	w := &response{
		conn:   c,
		writer: bytes.NewBuffer(nil),
		req:    &request{Body: buildWriteBody(t, handle, 0, []byte("hello world"))},
	}
	if err := onWrite(context.Background(), w, handler); err != nil {
		t.Fatalf("onWrite happy path: %v", err)
	}
	c.drainCaches()

	got, err := os.ReadFile(dir + "/data.bin")
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != "hello world" {
		t.Fatalf("content = %q, want %q", got, "hello world")
	}
}

// TestOnWriteNonexistentFileNoEnt verifies a WRITE to a path that does not
// exist returns NFSStatusNoEnt. With the pre-op path Stat removed, existence is
// now enforced by cachedWrite's O_RDWR open; the open's not-exist error must
// still map to NoEnt (statusFromWriteError), not a generic I/O error.
func TestOnWriteNonexistentFileNoEnt(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)

	handle := []byte("write-handle-0002")
	handler := &stubHandler{fs: fs, handle: handle, path: []string{"missing.bin"}}
	srv := &Server{Handler: handler}
	c := &conn{Server: srv}
	srv.registerConn(c)

	w := &response{
		conn:   c,
		writer: bytes.NewBuffer(nil),
		req:    &request{Body: buildWriteBody(t, handle, 0, []byte("data"))},
	}
	err := onWrite(context.Background(), w, handler)
	c.drainCaches()
	if err == nil {
		t.Fatal("expected error writing to nonexistent file, got nil")
	}
	var se *NFSStatusError
	if !errors.As(err, &se) || se.NFSStatus != NFSStatusNoEnt {
		t.Fatalf("expected NFSStatusNoEnt, got %v", err)
	}
}

// TestOnWriteDirectoryInval verifies a WRITE whose handle resolves to a
// directory is rejected. With the pre-op statRegularFile gone, the regular-file
// guard now comes from the opened fd's fstat (or the open failing); either way
// the result must be an error, not a successful write.
func TestOnWriteDirectoryInval(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)
	if err := fs.MkdirAll("adir", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	handle := []byte("write-handle-0003")
	handler := &stubHandler{fs: fs, handle: handle, path: []string{"adir"}}
	srv := &Server{Handler: handler}
	c := &conn{Server: srv}
	srv.registerConn(c)

	w := &response{
		conn:   c,
		writer: bytes.NewBuffer(nil),
		req:    &request{Body: buildWriteBody(t, handle, 0, []byte("data"))},
	}
	err := onWrite(context.Background(), w, handler)
	c.drainCaches()
	if err == nil {
		t.Fatal("expected error writing to a directory, got nil")
	}
}

