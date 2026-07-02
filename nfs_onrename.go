package nfs

import (
	"bytes"
	"context"
	"os"
	"reflect"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

var doubleWccErrorBody = [16]byte{}

func onRename(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = errFormatterWithBody(doubleWccErrorBody[:])
	from := DirOpArg{}
	err := xdr.Read(w.req.Body, &from)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	fs, fromPath, err := userHandle.FromHandle(ctx, from.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}

	to := DirOpArg{}
	if err = xdr.Read(w.req.Body, &to); err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	fs2, toPath, err := userHandle.FromHandle(ctx, to.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	// check the two fs are the same
	if !reflect.DeepEqual(fs, fs2) {
		return &NFSStatusError{NFSStatusNotSupp, os.ErrPermission}
	}

	if !billy.CapabilityCheck(fs, billy.WriteCapability) {
		return &NFSStatusError{NFSStatusROFS, os.ErrPermission}
	}

	if len(string(from.Filename)) > PathNameMax || len(string(to.Filename)) > PathNameMax {
		return &NFSStatusError{NFSStatusNameTooLong, os.ErrInvalid}
	}

	fromDirPath := fs.Join(fromPath...)
	fromDirInfo, err := fs.Stat(fromDirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		return &NFSStatusError{NFSStatusIO, err}
	}
	if !fromDirInfo.IsDir() {
		return &NFSStatusError{NFSStatusNotDir, nil}
	}
	preCacheData := ToFileAttribute(fromDirInfo, fromDirPath).AsCache()

	toDirPath := fs.Join(toPath...)
	toDirInfo, err := fs.Stat(toDirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		return &NFSStatusError{NFSStatusIO, err}
	}
	if !toDirInfo.IsDir() {
		return &NFSStatusError{NFSStatusNotDir, nil}
	}
	preDestData := ToFileAttribute(toDirInfo, toDirPath).AsCache()

	fromFile := append(fromPath, string(from.Filename))
	toFile := append(toPath, string(to.Filename))

	// resolveHandles re-resolves the cached handles for the source and destination
	// paths every time it is called, deduplicated (renaming a file onto itself
	// yields one). It is re-run rather than snapshotted once because a concurrent
	// LOOKUP can mint and a READ/WRITE can cache a handle for a path that was
	// uncached at an earlier call, in the window before fs.Rename: re-resolving
	// picks that up so the retry/invalidation drops it too.
	//
	// It prefers a lookup that does NOT allocate a new handle: a cached fd can only
	// exist under a handle the client already looked up, so a path with no existing
	// handle has nothing to drop. Allocating via ToHandle would mint a throwaway
	// handle (used only for invalidation) that a concurrent LOOKUP could observe
	// and then find stale, and could evict a live handle when the cache is full.
	resolveHandles := func() [][]byte {
		oldHandle := handleForInvalidation(ctx, userHandle, fs, fromFile)
		newHandle := handleForInvalidation(ctx, userHandle, fs, toFile)

		var handles [][]byte
		for _, h := range [][]byte{oldHandle, newHandle} {
			if h == nil {
				continue
			}
			if len(handles) == 1 && bytes.Equal(handles[0], h) {
				continue
			}
			handles = append(handles, h)
		}

		return handles
	}

	// Close any cached read/write fd for both the source and destination paths
	// BEFORE renaming, across all connections (another connection may hold its
	// own fd). On Windows and other backends that refuse to rename or replace an
	// open file, an fd left open on either side makes fs.Rename fail; on POSIX
	// this is harmless. Dropping also flushes pending unstable writes and ensures
	// no stale fd (pointing at the old/replaced inode) is reused afterwards.
	//
	// Capture these pre-rename handles: after a successful rename the source path
	// no longer exists, so a handler that resolves handles from the path (the
	// ToHandle fallback in handleForInvalidation, used when it does not implement
	// CachedHandleLookup) can no longer re-resolve the source below and would
	// never invalidate it — leaving a stateful handler aliasing the old handle to
	// a file later created at the source path. They are unioned into the
	// post-rename invalidation set so the source/replaced-destination handles are
	// invalidated regardless of handler type.
	preRenameHandles := resolveHandles()
	for _, h := range preRenameHandles {
		w.dropHandleAll(h)
	}

	fromLoc := fs.Join(fromFile...)
	toLoc := fs.Join(toFile...)

	// dropAndRetry guards the window between the pre-rename drop and fs.Rename: if
	// a racing READ/WRITE cached a fresh fd for either path there, the rename is
	// retried once after re-resolving and dropping the handles again (re-resolve,
	// so a path that was uncached above but has since been looked up is included).
	err = w.Server.dropAndRetry(func() error { return fs.Rename(fromLoc, toLoc) }, resolveHandles)
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		if os.IsPermission(err) {
			return &NFSStatusError{NFSStatusAccess, err}
		}
		return &NFSStatusError{NFSStatusIO, err}
	}

	// Invalidate the handler's path->handle mapping first, then drop. Once
	// InvalidateHandle runs, FromHandle can no longer resolve the handle, so no
	// further READ/WRITE can cache a new fd under it; the subsequent drop closes
	// any fd cached in the window since the pre-rename snapshot (including the
	// gap between fs.Rename and here). Dropping before invalidation would leave
	// that gap's fd open until idle cleanup. Re-resolve once more so a handle
	// minted for a previously-uncached path during the race window is invalidated
	// too, not just those seen at the first snapshot; union with the pre-rename
	// handles so a path-based handler's now-unresolvable source handle is still
	// invalidated (see preRenameHandles above).
	for _, h := range unionHandles(preRenameHandles, resolveHandles()) {
		if err := userHandle.InvalidateHandle(ctx, fs, h); err != nil {
			return &NFSStatusError{NFSStatusServerFault, err}
		}
		w.dropHandleAll(h)
	}

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := WriteWcc(writer, preCacheData, tryStat(fs, fromPath)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WriteWcc(writer, preDestData, tryStat(fs, toPath)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}

// unionHandles concatenates two handle lists, dropping duplicates (byte-equal
// handles), preserving order with the first list first. Used by RENAME to merge
// the pre-rename source/destination handles with the post-rename re-resolved
// ones so each handle is invalidated exactly once regardless of which snapshot
// saw it.
func unionHandles(a, b [][]byte) [][]byte {
	out := make([][]byte, 0, len(a)+len(b))
	seen := func(h []byte) bool {
		for _, e := range out {
			if bytes.Equal(e, h) {
				return true
			}
		}
		return false
	}
	for _, h := range a {
		if h != nil && !seen(h) {
			out = append(out, h)
		}
	}
	for _, h := range b {
		if h != nil && !seen(h) {
			out = append(out, h)
		}
	}

	return out
}

// handleForInvalidation returns a handle for path used solely to invalidate
// cached fds, without allocating a new one when the handler supports a
// cached-only lookup (CachedHandleLookup). It returns nil when no handle is
// currently cached for the path — meaning no fd can be cached under it, so there
// is nothing to invalidate.
//
// Handlers without the cached-only capability fall back to ToHandle, but only
// for a path that EXISTS: a stateless/custom handler that derives handles from
// file metadata (device+inode, which the Handler docs allow) cannot construct a
// valid handle for a missing path and may fail or panic. A missing path also has
// no cached fd to drop, so returning nil there is both safe and correct — this
// matters for a normal rename to a brand-new destination filename, whose target
// does not exist yet.
func handleForInvalidation(ctx context.Context, h Handler, fs billy.Filesystem, path []string) []byte {
	if lookup, ok := h.(CachedHandleLookup); ok {
		return lookup.HandleForPathIfCached(fs, path)
	}
	if _, err := fs.Stat(fs.Join(path...)); err != nil {
		return nil
	}

	return h.ToHandle(ctx, fs, path)
}
