package nfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/willscott/go-nfs/helpers/memfs"
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
// directory is rejected. With the pre-op statRegularFile gone, the directory
// open fails with EISDIR, which must be mapped to a client-facing status
// (NFS3ERR_ISDIR) rather than a generic server I/O error.
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
	// Must be a client-facing status, not a generic server I/O error.
	var se *NFSStatusError
	if !errors.As(err, &se) || se.NFSStatus == NFSStatusIO {
		t.Fatalf("expected a client-facing status (e.g. NFSStatusIsDir), got %v", err)
	}
}

// TestOnWriteDirectoryNonEISDIRBackend verifies the directory-write status is
// client-facing even on backends whose OpenFile does NOT return an EISDIR-
// wrapping error. helpers/memfs returns a plain fmt.Errorf("cannot open
// directory: ...") for an O_RDWR open of a directory, which the EISDIR-only
// mapping would miss; cachedWrite classifies the directory target explicitly so
// the client still sees a non-IO status.
func TestOnWriteDirectoryNonEISDIRBackend(t *testing.T) {
	fs := memfs.New()
	if err := fs.MkdirAll("adir", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	handle := []byte("write-handle-mdir")
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
	var se *NFSStatusError
	if !errors.As(err, &se) || se.NFSStatus == NFSStatusIO {
		t.Fatalf("expected a client-facing status, got %v", err)
	}
}


// nonRegularStatFile is a billy.File whose fstat reports a non-regular mode
// (e.g. a device node), while WriteAt records whether it was ever called. It
// simulates a backend where the WRITE target's handle resolves to a special
// file that can be opened O_RDWR — the case the cold-open type guard in
// cachedWrite must reject before any write side effect.
type nonRegularStatFile struct {
	billy.File
	mode    os.FileMode
	written *bool
}

func (f *nonRegularStatFile) Stat() (os.FileInfo, error) {
	return nonRegularFileInfo{mode: f.mode}, nil
}

func (f *nonRegularStatFile) WriteAt(p []byte, off int64) (int, error) {
	*f.written = true
	return len(p), nil
}

type nonRegularFileInfo struct {
	mode os.FileMode
}

func (i nonRegularFileInfo) Name() string       { return "special" }
func (i nonRegularFileInfo) Size() int64         { return 0 }
func (i nonRegularFileInfo) Mode() os.FileMode   { return i.mode }
func (i nonRegularFileInfo) ModTime() time.Time  { return time.Time{} }
func (i nonRegularFileInfo) IsDir() bool         { return i.mode.IsDir() }
func (i nonRegularFileInfo) Sys() any            { return nil }

type nonRegularFS struct {
	billy.Filesystem
	mode    os.FileMode
	written *bool
}

func (fs nonRegularFS) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	f, err := fs.Filesystem.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &nonRegularStatFile{File: f, mode: fs.mode, written: fs.written}, nil
}

// noStatFile is a billy.File that deliberately exposes NO Stat method (it embeds
// the billy.File interface, whose method set has no Stat, so any concrete Stat
// on the underlying file is hidden). It records whether WriteAt was called. It
// models a backend like stock osfs whose file cannot be fstat'd, so the WRITE
// type guard must fall back to a path Stat.
type noStatFile struct {
	billy.File
	written *bool
}

func (f *noStatFile) WriteAt(p []byte, off int64) (int, error) {
	*f.written = true
	return len(p), nil
}

// noStatFS returns files without a Stat method, but reports a caller-chosen mode
// from the filesystem-level Stat, exercising the path-Stat fallback in the type
// guard.
type noStatFS struct {
	billy.Filesystem
	statMode os.FileMode
	written  *bool
}

func (fs noStatFS) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	f, err := fs.Filesystem.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &noStatFile{File: f, written: fs.written}, nil
}

func (fs noStatFS) Stat(name string) (os.FileInfo, error) {
	return nonRegularFileInfo{mode: fs.statMode}, nil
}

// TestOnWriteRejectsNonRegularViaPathStatFallback verifies the type guard still
// holds when the opened billy.File has no Stat method (e.g. stock osfs): it
// falls back to a path Stat, and a non-regular target (device) is rejected with
// NFSStatusInval before any write reaches the fd.
func TestOnWriteRejectsNonRegularViaPathStatFallback(t *testing.T) {
	dir := t.TempDir()
	base := osfs.New(dir)
	if f, err := base.Create("dev.bin"); err == nil {
		_ = f.Close()
	}

	written := false
	fs := noStatFS{Filesystem: base, statMode: os.ModeDevice | 0o644, written: &written}

	handle := []byte("write-handle-dev2")
	handler := &stubHandler{fs: fs, handle: handle, path: []string{"dev.bin"}}
	srv := &Server{Handler: handler}
	c := &conn{Server: srv}
	srv.registerConn(c)

	w := &response{
		conn:   c,
		writer: bytes.NewBuffer(nil),
		req:    &request{Body: buildWriteBody(t, handle, 0, []byte("payload"))},
	}
	err := onWrite(context.Background(), w, handler)
	c.drainCaches()
	if err == nil {
		t.Fatal("expected error writing to a non-regular file (path-Stat fallback), got nil")
	}
	var se *NFSStatusError
	if !errors.As(err, &se) || se.NFSStatus != NFSStatusInval {
		t.Fatalf("expected NFSStatusInval, got %v", err)
	}
	if written {
		t.Fatal("write reached the fd; path-Stat fallback must reject before writing")
	}
}

// TestOnWriteRejectsNonRegularFdBeforeWrite verifies the cold-open type guard:
// a WRITE whose freshly-opened fd fstats as a non-regular file (device/FIFO/
// socket) is rejected with NFSStatusInval and no bytes are written to it,
// restoring the protection the removed pre-op path Stat used to provide.
func TestOnWriteRejectsNonRegularFdBeforeWrite(t *testing.T) {
	dir := t.TempDir()
	base := osfs.New(dir)
	if f, err := base.Create("dev.bin"); err == nil {
		_ = f.Close()
	}

	written := false
	fs := nonRegularFS{Filesystem: base, mode: os.ModeDevice | 0o644, written: &written}

	handle := []byte("write-handle-dev1")
	handler := &stubHandler{fs: fs, handle: handle, path: []string{"dev.bin"}}
	srv := &Server{Handler: handler}
	c := &conn{Server: srv}
	srv.registerConn(c)

	w := &response{
		conn:   c,
		writer: bytes.NewBuffer(nil),
		req:    &request{Body: buildWriteBody(t, handle, 0, []byte("payload"))},
	}
	err := onWrite(context.Background(), w, handler)
	c.drainCaches()
	if err == nil {
		t.Fatal("expected error writing to a non-regular file, got nil")
	}
	var se *NFSStatusError
	if !errors.As(err, &se) || se.NFSStatus != NFSStatusInval {
		t.Fatalf("expected NFSStatusInval, got %v", err)
	}
	if written {
		t.Fatal("write reached a non-regular fd; type guard must reject before writing")
	}
}


// rotateOnWriteFile is a billy.File whose WriteAt succeeds but rotates the
// server write verifier as a side effect, simulating a concurrent eviction
// whose failed close-flush triggers rotateWriteVerifier after the write lands
// but before the reply is built.
type rotateOnWriteFile struct {
	billy.File
	srv *Server
}

func (f *rotateOnWriteFile) WriteAt(p []byte, off int64) (int, error) {
	f.srv.rotateWriteVerifier()
	return len(p), nil
}

func (f *rotateOnWriteFile) Stat() (os.FileInfo, error) { return nil, os.ErrInvalid }

// rotateFS wraps a billy.Filesystem so OpenFile returns a rotateOnWriteFile,
// forcing the verifier to rotate during cachedWrite.
type rotateFS struct {
	billy.Filesystem
	srv *Server
}

func (fs rotateFS) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	f, err := fs.Filesystem.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &rotateOnWriteFile{File: f, srv: fs.srv}, nil
}

// TestOnWriteStampsPreWriteVerifier verifies an UNSTABLE WRITE reply carries the
// verifier captured BEFORE the write, so a rotation triggered by a
// concurrent-loss during the write does not stamp the new verifier onto this
// reply (which would let a later COMMIT compare equal and skip replay).
func TestOnWriteStampsPreWriteVerifier(t *testing.T) {
	dir := t.TempDir()
	base := osfs.New(dir)
	if f, err := base.Create("data.bin"); err == nil {
		_ = f.Close()
	}

	handle := []byte("write-handle-rot1")
	srv := &Server{}
	fs := rotateFS{Filesystem: base, srv: srv}
	handler := &stubHandler{fs: fs, handle: handle, path: []string{"data.bin"}}
	srv.Handler = handler
	c := &conn{Server: srv}
	srv.registerConn(c)

	before := srv.currentWriteVerifier()

	w := &response{
		conn:   c,
		writer: bytes.NewBuffer(nil),
		req:    &request{Body: buildWriteBody(t, handle, 0, []byte("payload"))},
	}
	if err := onWrite(context.Background(), w, handler); err != nil {
		t.Fatalf("onWrite: %v", err)
	}
	c.drainCaches()

	after := srv.currentWriteVerifier()
	if after == before {
		t.Fatal("verifier did not rotate; test cannot exercise the hole")
	}

	// The reply's trailing 8 bytes are the write verifier; they must equal the
	// pre-write value, not the rotated one.
	reply := w.writer.Bytes()
	if len(reply) < 8 {
		t.Fatalf("reply too short: %d bytes", len(reply))
	}
	var got [8]byte
	copy(got[:], reply[len(reply)-8:])
	if got != before {
		t.Fatalf("reply verifier = %x, want pre-write %x (rotated is %x)", got, before, after)
	}
}
