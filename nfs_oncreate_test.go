package nfs

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// buildCreateBody builds a CREATE request body: a DirOpArg (dir handle + the
// filename), createMode=unchecked, then an empty SetFileAttributes (all
// has-fields zero, so Apply is a no-op).
func buildCreateBody(t *testing.T, dirHandle []byte, filename string) *bytes.Buffer {
	t.Helper()
	body := bytes.NewBuffer(nil)
	// DirOpArg: opaque dir handle, then the filename as opaque<>.
	if err := xdr.Write(body, dirHandle); err != nil {
		t.Fatalf("write dir handle: %v", err)
	}
	if err := xdr.Write(body, []byte(filename)); err != nil {
		t.Fatalf("write filename: %v", err)
	}
	// how = createModeUnchecked.
	if err := xdr.Write(body, uint32(createModeUnchecked)); err != nil {
		t.Fatalf("write how: %v", err)
	}
	// SetFileAttributes: hasMode/hasUID/hasGID/hasSize/atime/mtime all 0.
	for i := 0; i < 6; i++ {
		if err := xdr.Write(body, uint32(0)); err != nil {
			t.Fatalf("write attr field: %v", err)
		}
	}

	return body
}

// CREATE in unchecked mode on an EXISTING file is a truncating overwrite
// (fs.Create is O_CREATE|O_TRUNC|O_RDWR). A cached dirty write handle for that
// file — here on a second connection, as nconnect produces — must be dropped
// (and its fd closed) so its pending unstable writes cannot be flushed after the
// truncate and resurrect content. Without the fix the peer handle survives.
func TestOnCreateTruncateDropsCachedHandlesAllConns(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)

	name := "f.dat"
	f, err := fs.Create(name)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := f.Write([]byte("preexisting")); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = f.Close()

	// 16-byte handle (XDR opaque alignment, see onSetAttr test). The stub maps
	// both the dir handle and the new-file handle to this same value, and path
	// to root, so newFilePath == name and the cached key matches fp.
	handle := []byte("create-handle16!")
	handler := &stubHandler{fs: fs, handle: handle, path: []string{}}

	srv := &Server{Handler: handler}
	c1 := &conn{Server: srv} // receives CREATE
	c2 := &conn{Server: srv} // holds the dirty cached write handle
	srv.registerConn(c1)
	srv.registerConn(c2)

	wf, err := fs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open w: %v", err)
	}
	peer := &cachedHandle{file: wf, dirty: true, lastUsed: time.Now()}
	c2.writeHandleCache().put(string(handle), peer)

	w := &response{
		conn:   c1,
		writer: bytes.NewBuffer(nil),
		req:    &request{Body: buildCreateBody(t, handle, name)},
	}

	if err := onCreate(context.Background(), w, handler); err != nil {
		t.Fatalf("onCreate: %v", err)
	}

	// The peer connection's cached handle must be gone and its fd closed, so its
	// pending writes can never be flushed onto the truncated file.
	if c2.wc.get(string(handle)) != nil {
		t.Fatal("peer write handle still cached after truncating CREATE")
	}
	if _, err := peer.writeAt([]byte("x"), 0); err == nil {
		t.Fatal("expected peer cached write fd to be closed after truncating CREATE")
	}

	// The file on disk is truncated (empty), not resurrected.
	got, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("readfile: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected truncated file, got %q", got)
	}

	c1.drainCaches()
	c2.drainCaches()
}

// CREATE of a brand-new file must not error when no cached handle exists (the
// pre/post drops are gated on the file having existed).
func TestOnCreateNewFileNoExistingCache(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)

	handle := []byte("create-handle16!")
	handler := &stubHandler{fs: fs, handle: handle, path: []string{}}
	srv := &Server{Handler: handler}
	c := &conn{Server: srv}
	srv.registerConn(c)

	w := &response{
		conn:   c,
		writer: bytes.NewBuffer(nil),
		req:    &request{Body: buildCreateBody(t, handle, "new.dat")},
	}
	if err := onCreate(context.Background(), w, handler); err != nil {
		t.Fatalf("onCreate new file: %v", err)
	}
	if _, err := fs.Stat("new.dat"); err != nil {
		t.Fatalf("expected new file created: %v", err)
	}
	c.drainCaches()
}

// createOrderHandler records, at each ToHandle call, whether the target file
// already exists on disk. It lets a test assert that CREATE of a brand-new file
// does not mint a handle before fs.Create has made the file — handlers that
// derive handles from file metadata (device+inode) cannot form a valid handle
// for a path that does not exist yet.
type createOrderHandler struct {
	*stubHandler
	missingAtToHandle bool // ToHandle saw the target path not existing
}

func (h *createOrderHandler) ToHandle(_ context.Context, fs billy.Filesystem, path []string) []byte {
	if _, err := fs.Stat(fs.Join(path...)); os.IsNotExist(err) {
		h.missingAtToHandle = true
	}
	return h.handle
}

// CREATE of a brand-new file must not call ToHandle before fs.Create exists the
// file: an inode/device-derived handler could only return a nil/invalid handle
// for a nonexistent path, which would then be published and returned to the
// client. Regression guard for hoisting ToHandle ahead of fs.Create.
func TestOnCreateNewFileHandleMintedAfterCreate(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)

	handle := []byte("create-handle16!")
	h := &createOrderHandler{stubHandler: &stubHandler{fs: fs, handle: handle, path: []string{}}}
	srv := &Server{Handler: h}
	c := &conn{Server: srv}
	srv.registerConn(c)

	w := &response{
		conn:   c,
		writer: bytes.NewBuffer(nil),
		req:    &request{Body: buildCreateBody(t, handle, "fresh.dat")},
	}
	if err := onCreate(context.Background(), w, h); err != nil {
		t.Fatalf("onCreate new file: %v", err)
	}
	if h.missingAtToHandle {
		t.Fatal("ToHandle was called before the new file existed; handle must be minted after fs.Create")
	}
	if _, err := fs.Stat("fresh.dat"); err != nil {
		t.Fatalf("expected new file created: %v", err)
	}
	c.drainCaches()
}

// buildCreateBodyMode builds a CREATE body that sets mode (hasMode=1), so
// attrs.Apply chmods the file. Used to verify CREATE invalidates cached handles
// AFTER applying attributes (like SETATTR), so an fd cached in the create→Apply
// window cannot keep bypassing the just-set mode.
func buildCreateBodyMode(t *testing.T, dirHandle []byte, filename string, mode uint32) *bytes.Buffer {
	t.Helper()
	body := bytes.NewBuffer(nil)
	if err := xdr.Write(body, dirHandle); err != nil {
		t.Fatalf("write dir handle: %v", err)
	}
	if err := xdr.Write(body, []byte(filename)); err != nil {
		t.Fatalf("write filename: %v", err)
	}
	if err := xdr.Write(body, uint32(createModeUnchecked)); err != nil {
		t.Fatalf("write how: %v", err)
	}
	// SetFileAttributes: hasMode=1, mode, then hasUID/hasGID/hasSize/atime/mtime=0.
	if err := xdr.Write(body, uint32(1)); err != nil {
		t.Fatalf("write hasMode: %v", err)
	}
	if err := xdr.Write(body, mode); err != nil {
		t.Fatalf("write mode: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := xdr.Write(body, uint32(0)); err != nil {
			t.Fatalf("write attr field: %v", err)
		}
	}

	return body
}

// CREATE that sets attributes must invalidate cached handles AFTER applying
// them, like SETATTR. A peer connection can cache an O_RDWR fd (opened under the
// default create mode) in the window between create and Apply; backends check
// permissions at open() time, so without a post-Apply drop that fd would keep
// writing despite attributes CREATE just set. This models that fd as already
// cached and asserts onCreate drops it after Apply. (Apply uses the file's
// current mode so the chmod is a no-op: osfs's ChrootHelper doesn't satisfy the
// full billy.Change interface, and the fix under test is the invalidation, not
// chmod itself — mirrors TestOnSetAttrDropsCachedHandlesAllConns.)
func TestOnCreateModeInvalidatesHandleAfterApply(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)

	name := "f.dat"
	// Pre-create the file so the existed-path runs and we can seed a peer fd that
	// stands in for one cached in the create→Apply window.
	f, err := fs.Create(name)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = f.Close()
	fi, err := fs.Stat(name)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	curMode := uint32(fi.Mode().Perm())

	handle := []byte("create-handle16!")
	handler := &stubHandler{fs: fs, handle: handle, path: []string{}}
	srv := &Server{Handler: handler}
	c1 := &conn{Server: srv} // receives CREATE
	c2 := &conn{Server: srv} // holds a cached fd opened under the old mode
	srv.registerConn(c1)
	srv.registerConn(c2)

	wf, err := fs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open w: %v", err)
	}
	peer := &cachedHandle{file: wf, lastUsed: time.Now()}
	c2.writeHandleCache().put(string(handle), peer)

	w := &response{
		conn:   c1,
		writer: bytes.NewBuffer(nil),
		req:    &request{Body: buildCreateBodyMode(t, handle, name, curMode)},
	}
	if err := onCreate(context.Background(), w, handler); err != nil {
		t.Fatalf("onCreate: %v", err)
	}

	// Post-Apply invalidation must have dropped the peer's cached fd, forcing the
	// next WRITE to reopen and be re-checked against the applied attributes.
	if c2.wc.get(string(handle)) != nil {
		t.Fatal("peer write handle still cached after attribute-setting CREATE")
	}
	if _, err := peer.writeAt([]byte("x"), 0); err == nil {
		t.Fatal("expected peer cached write fd to be closed after CREATE applied attributes")
	}

	c1.drainCaches()
	c2.drainCaches()
}
