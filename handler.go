package nfs

import (
	"context"
	"io/fs"
	"net"

	billy "github.com/go-git/go-billy/v5"
)

// Handler represents the interface of the file system / vfs being exposed over NFS
type Handler interface {
	// Required methods

	Mount(context.Context, net.Conn, MountRequest) (MountStatus, billy.Filesystem, []AuthFlavor)

	// Change can return 'nil' if filesystem is read-only
	// If the returned value can be cast to `UnixChange`, mknod and link RPCs will be available.
	Change(context.Context, billy.Filesystem) billy.Change

	// Optional methods - generic helpers or trivial implementations can be sufficient depending on use case.

	// Fill in information about a file system's free space.
	FSStat(context.Context, billy.Filesystem, *FSStat) error

	// represent file objects as opaque references
	// Can be safely implemented via helpers/cachinghandler.
	ToHandle(cxt context.Context, fs billy.Filesystem, path []string) []byte
	FromHandle(ctx context.Context, fh []byte) (billy.Filesystem, []string, error)
	InvalidateHandle(context.Context, billy.Filesystem, []byte) error

	// How many handles can be safely maintained by the handler.
	HandleLimit() int
}

// CachedHandleLookup is an optional Handler capability: returning an existing
// handle for a path without allocating a new one. Handle-mutating ops (RENAME)
// use it to invalidate cached fds for a destination path that may never have
// been looked up, avoiding ToHandle's side effects — minting a throwaway handle
// a concurrent LOOKUP could receive and then find stale, or evicting a live
// handle when the handle cache is full. A nil result means no handle is cached
// for that path, so there is nothing to invalidate. Handlers that do not
// implement this fall back to ToHandle.
type CachedHandleLookup interface {
	HandleForPathIfCached(fs billy.Filesystem, path []string) []byte
}

// HandleEvictionReceiver is an optional Handler capability for handlers with a
// bounded handle cache (e.g. helpers.CachingHandler). The server registers a
// callback via SetHandleEvictionCallback; the handler must invoke it whenever it
// evicts a handle from its cache. The server uses it to close any per-connection
// read/write fd still cached under that opaque handle.
//
// Without this, the handle cache and the per-connection fd caches evict
// independently: a handle dropped from the handler's cache leaves its fd open
// until the fd cache's own idle sweep or a COMMIT. In that window a RENAME/REMOVE
// can no longer resolve the path to that handle (CachedHandleLookup misses, since
// the handle is gone), so it cannot drop the fd — and a Windows-like backend that
// refuses to rename/replace an open file fails. Coupling fd lifetime to handle
// eviction closes that window.
type HandleEvictionReceiver interface {
	SetHandleEvictionCallback(func(handle []byte))
}

// UnixChange extends the billy `Change` interface with support for special files.
type UnixChange interface {
	billy.Change
	Mknod(path string, mode uint32, major uint32, minor uint32) error
	Mkfifo(path string, mode uint32) error
	Socket(path string) error
	Link(path string, link string) error
}

// CachingHandler represents the optional caching work that a user may wish to over-ride with
// their own implementations, but which can be otherwise provided through defaults.
type CachingHandler interface {
	VerifierFor(path string, contents []fs.FileInfo) uint64

	// fs.FileInfo needs to be sorted by Name(), nil in case of a cache-miss
	DataForVerifier(path string, verifier uint64) []fs.FileInfo
}
