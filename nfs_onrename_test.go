package nfs

import (
	"bytes"
	"context"
	"net"
	"testing"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// renameHandler drives onRename in-package. It maps the root dir handle, tracks
// ToHandle allocations, and implements CachedHandleLookup so a path only yields
// a handle if it was explicitly seeded as "cached".
type renameHandler struct {
	fs          billy.Filesystem
	dirHandle   []byte
	cached      map[string][]byte // path -> handle for paths with a live cached handle
	toHandleN   int               // count of ToHandle (allocating) calls
	invalidated [][]byte
}

func (h *renameHandler) Mount(context.Context, net.Conn, MountRequest) (MountStatus, billy.Filesystem, []AuthFlavor) {
	return MountStatusOk, h.fs, nil
}
func (h *renameHandler) Change(_ context.Context, fs billy.Filesystem) billy.Change {
	if c, ok := fs.(billy.Change); ok {
		return c
	}
	return nil
}
func (h *renameHandler) FSStat(context.Context, billy.Filesystem, *FSStat) error { return nil }
func (h *renameHandler) ToHandle(_ context.Context, _ billy.Filesystem, path []string) []byte {
	h.toHandleN++
	// Allocate a fresh, distinct handle (mirrors CachingHandler minting a UUID).
	return []byte("alloc-handle-" + h.fs.Join(path...))
}
func (h *renameHandler) FromHandle(_ context.Context, fh []byte) (billy.Filesystem, []string, error) {
	if bytes.Equal(fh, h.dirHandle) {
		return h.fs, []string{}, nil
	}
	return nil, nil, errInvalidRenameHandle
}
func (h *renameHandler) InvalidateHandle(_ context.Context, _ billy.Filesystem, handle []byte) error {
	h.invalidated = append(h.invalidated, handle)
	return nil
}
func (h *renameHandler) HandleLimit() int { return 1024 }

// HandleForPathIfCached returns a handle only for seeded paths; nil otherwise,
// without allocating.
func (h *renameHandler) HandleForPathIfCached(fs billy.Filesystem, path []string) []byte {
	return h.cached[fs.Join(path...)]
}

var errInvalidRenameHandle = &NFSStatusError{NFSStatusStale, nil}

func buildRenameBody(t *testing.T, dirHandle []byte, fromName, toName string) *bytes.Buffer {
	t.Helper()
	body := bytes.NewBuffer(nil)
	// from DirOpArg
	if err := xdr.Write(body, dirHandle); err != nil {
		t.Fatalf("write from handle: %v", err)
	}
	if err := xdr.Write(body, []byte(fromName)); err != nil {
		t.Fatalf("write from name: %v", err)
	}
	// to DirOpArg
	if err := xdr.Write(body, dirHandle); err != nil {
		t.Fatalf("write to handle: %v", err)
	}
	if err := xdr.Write(body, []byte(toName)); err != nil {
		t.Fatalf("write to name: %v", err)
	}

	return body
}

// RENAME must not allocate (ToHandle) a fresh handle for the destination path
// when the handler supports cached-only lookup: a throwaway handle would be
// publishable to a concurrent LOOKUP and then immediately invalidated (stale),
// and could evict a live handle. With no cached handle on either side there is
// nothing to invalidate.
func TestOnRenameNoHandleAllocation(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)
	if f, err := fs.Create("src.dat"); err != nil {
		t.Fatalf("create: %v", err)
	} else {
		_ = f.Close()
	}

	dirHandle := []byte("rename-dir-handl")
	h := &renameHandler{fs: fs, dirHandle: dirHandle, cached: map[string][]byte{}}
	srv := &Server{Handler: h}
	c := &conn{Server: srv}
	srv.registerConn(c)
	defer c.drainCaches()

	w := &response{
		conn:   c,
		writer: bytes.NewBuffer(nil),
		req:    &request{Body: buildRenameBody(t, dirHandle, "src.dat", "dst.dat")},
	}
	if err := onRename(context.Background(), w, h); err != nil {
		t.Fatalf("onRename: %v", err)
	}

	if h.toHandleN != 0 {
		t.Fatalf("ToHandle called %d times; must not allocate a handle for uncached paths", h.toHandleN)
	}
	if len(h.invalidated) != 0 {
		t.Fatalf("invalidated %d handles; nothing was cached so nothing to invalidate", len(h.invalidated))
	}
	// The rename itself must have happened.
	if _, err := fs.Stat("dst.dat"); err != nil {
		t.Fatalf("expected renamed file: %v", err)
	}
}

// When a path DOES have a cached handle, RENAME invalidates exactly it (still
// without allocating).
func TestOnRenameInvalidatesCachedHandleOnly(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)
	if f, err := fs.Create("src.dat"); err != nil {
		t.Fatalf("create: %v", err)
	} else {
		_ = f.Close()
	}

	dirHandle := []byte("rename-dir-handl")
	srcHandle := []byte("src-cached-handl")
	h := &renameHandler{
		fs:        fs,
		dirHandle: dirHandle,
		cached:    map[string][]byte{fs.Join("src.dat"): srcHandle},
	}
	srv := &Server{Handler: h}
	c := &conn{Server: srv}
	srv.registerConn(c)
	defer c.drainCaches()

	w := &response{
		conn:   c,
		writer: bytes.NewBuffer(nil),
		req:    &request{Body: buildRenameBody(t, dirHandle, "src.dat", "dst.dat")},
	}
	if err := onRename(context.Background(), w, h); err != nil {
		t.Fatalf("onRename: %v", err)
	}

	if h.toHandleN != 0 {
		t.Fatalf("ToHandle called %d times; must use cached lookup", h.toHandleN)
	}
	if len(h.invalidated) != 1 || !bytes.Equal(h.invalidated[0], srcHandle) {
		t.Fatalf("invalidated = %v, want exactly [%q]", h.invalidated, srcHandle)
	}
}
