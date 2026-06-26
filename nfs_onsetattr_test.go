package nfs

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	billy "github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// stubHandler is a minimal Handler for driving handlers directly in-package
// (importing helpers would create an import cycle). It resolves a single
// fixed handle to a fixed path and delegates Change to the filesystem.
type stubHandler struct {
	fs         billy.Filesystem
	handle     []byte
	path       []string
	invalidate func(handle []byte) error
}

func (h *stubHandler) Mount(context.Context, net.Conn, MountRequest) (MountStatus, billy.Filesystem, []AuthFlavor) {
	return MountStatusOk, h.fs, nil
}
func (h *stubHandler) Change(_ context.Context, fs billy.Filesystem) billy.Change {
	if c, ok := fs.(billy.Change); ok {
		return c
	}
	return nil
}
func (h *stubHandler) FSStat(context.Context, billy.Filesystem, *FSStat) error { return nil }
func (h *stubHandler) ToHandle(context.Context, billy.Filesystem, []string) []byte {
	return h.handle
}
func (h *stubHandler) FromHandle(_ context.Context, fh []byte) (billy.Filesystem, []string, error) {
	if !bytes.Equal(fh, h.handle) {
		return nil, nil, errors.New("unknown handle")
	}
	return h.fs, h.path, nil
}
func (h *stubHandler) InvalidateHandle(_ context.Context, _ billy.Filesystem, handle []byte) error {
	if h.invalidate != nil {
		return h.invalidate(handle)
	}
	return nil
}
func (h *stubHandler) HandleLimit() int { return 1024 }

// buildSetAttrBody builds a SETATTR request body: opaque handle, then a
// SetFileAttributes with only SetMode present, then guard=0 (no ctime guard).
func buildSetAttrBody(t *testing.T, handle []byte, mode uint32) *bytes.Buffer {
	t.Helper()
	body := bytes.NewBuffer(nil)
	if err := xdr.Write(body, handle); err != nil { // opaque<>
		t.Fatalf("write handle: %v", err)
	}
	// SetFileAttributes: hasMode=1, mode, then hasUID/hasGID/hasSize/atime/mtime = 0.
	for _, v := range []uint32{1, mode, 0, 0, 0, 0, 0} {
		if err := xdr.Write(body, v); err != nil {
			t.Fatalf("write attr field: %v", err)
		}
	}
	// guard: check=0 (no guard).
	if err := xdr.Write(body, uint32(0)); err != nil {
		t.Fatalf("write guard: %v", err)
	}

	return body
}

// onSetAttr must drop the file's cached read/write fds so a chmod that changes
// permissions is enforced on the next WRITE/READ (which reopens) rather than
// being bypassed by a still-open O_RDWR handle.
func TestOnSetAttrDropsCachedHandlesAllConns(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)

	name := "f.dat"
	f, err := fs.Create(name)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = f.Close()
	fi, err := fs.Stat(name)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	// Use the file's current mode so Apply's chmod is a no-op: this test
	// exercises the cache drop (which happens before Apply), not chmod itself,
	// and osfs's ChrootHelper doesn't satisfy the full billy.Change interface.
	curMode := uint32(fi.Mode().Perm())

	// 16-byte handle: XDR opaque pads to a 4-byte boundary and ReadOpaque does
	// not consume padding, so an unaligned length would desync the body parse.
	// Real handles (CachingHandler UUIDs) are 16 bytes, so this mirrors them.
	handle := []byte("setattr-handle16")
	handler := &stubHandler{fs: fs, handle: handle, path: []string{name}}

	srv := &Server{Handler: handler}
	// Two live connections, as nconnect or a second client would produce. Both
	// cache an fd for the same file handle; a SETATTR on one must invalidate
	// both so neither reuses an fd opened under the old attributes.
	c1 := &conn{Server: srv}
	c2 := &conn{Server: srv}
	srv.registerConn(c1)
	srv.registerConn(c2)

	seedWrite := func(c *conn) *cachedHandle {
		wf, err := fs.OpenFile(name, os.O_RDWR, 0o644)
		if err != nil {
			t.Fatalf("open w: %v", err)
		}
		h := &cachedHandle{file: wf, lastUsed: time.Now()}
		c.writeHandleCache().put(string(handle), h)

		return h
	}
	seedRead := func(c *conn) *readHandle {
		rf, err := fs.OpenFile(name, os.O_RDONLY, 0)
		if err != nil {
			t.Fatalf("open r: %v", err)
		}
		h := &readHandle{file: rf}
		h.lastUsed.Store(time.Now().UnixNano())
		c.readHandleCache().put(string(handle), h)

		return h
	}

	wh1, rh1 := seedWrite(c1), seedRead(c1)
	wh2, rh2 := seedWrite(c2), seedRead(c2)

	w := &response{
		conn:   c1,
		writer: bytes.NewBuffer(nil),
		req:    &request{Body: buildSetAttrBody(t, handle, curMode)},
	}

	if err := onSetAttr(context.Background(), w, handler); err != nil {
		t.Fatalf("onSetAttr: %v", err)
	}

	// Both connections' caches must be cleared, not just the handling one.
	for i, c := range []*conn{c1, c2} {
		if c.wc.get(string(handle)) != nil {
			t.Fatalf("conn %d: write handle still cached after onSetAttr", i+1)
		}
		if c.rc.get(string(handle)) != nil {
			t.Fatalf("conn %d: read handle still cached after onSetAttr", i+1)
		}
	}
	// Every dropped fd must be closed.
	for i, wh := range []*cachedHandle{wh1, wh2} {
		if _, err := wh.writeAt([]byte("x"), 0); err == nil {
			t.Fatalf("conn %d: expected cached write fd closed after onSetAttr", i+1)
		}
	}
	for i, rh := range []*readHandle{rh1, rh2} {
		if _, err := rh.readAt(make([]byte, 1), 0); err == nil {
			t.Fatalf("conn %d: expected cached read fd closed after onSetAttr", i+1)
		}
	}

	c1.drainCaches()
	c2.drainCaches()
}
