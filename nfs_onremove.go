package nfs

import (
	"bytes"
	"context"
	"os"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

func onRemove(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = wccDataErrorFormatter
	obj := DirOpArg{}
	if err := xdr.Read(w.req.Body, &obj); err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	fs, path, err := userHandle.FromHandle(ctx, obj.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}

	if !billy.CapabilityCheck(fs, billy.WriteCapability) {
		return &NFSStatusError{NFSStatusROFS, os.ErrPermission}
	}

	if len(string(obj.Filename)) > PathNameMax {
		return &NFSStatusError{NFSStatusNameTooLong, nil}
	}

	fullPath := fs.Join(path...)
	dirInfo, err := fs.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		if os.IsPermission(err) {
			return &NFSStatusError{NFSStatusAccess, err}
		}
		return &NFSStatusError{NFSStatusIO, err}
	}
	if !dirInfo.IsDir() {
		return &NFSStatusError{NFSStatusNotDir, nil}
	}
	preCacheData := ToFileAttribute(dirInfo, fullPath).AsCache()

	toDelete := fs.Join(append(path, string(obj.Filename))...)
	toDeleteHandle := userHandle.ToHandle(ctx, fs, append(path, string(obj.Filename)))

	// Close any cached read/write fd for this file before removing it, across
	// all connections (another connection may hold its own fd). On POSIX this is
	// unnecessary (unlink of an open file succeeds), but Windows and other
	// backends refuse to delete a file that is still open, so a cached fd would
	// make fs.Remove fail. Closing first also flushes any pending unstable
	// writes, which is harmless for a file about to vanish. dropAndRetry guards
	// the window between this drop and fs.Remove: if a racing READ/WRITE cached
	// a fresh fd there, the removal is retried once after dropping again.
	w.dropHandleAll(toDeleteHandle)

	err = w.Server.dropAndRetry(func() error { return fs.Remove(toDelete) }, func() [][]byte { return [][]byte{toDeleteHandle} })
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
	// any fd cached in the window since the pre-remove snapshot (including the
	// gap between fs.Remove and here). Dropping before invalidation would leave
	// that gap's fd open until idle cleanup.
	if err := userHandle.InvalidateHandle(ctx, fs, toDeleteHandle); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	w.dropHandleAll(toDeleteHandle)

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := WriteWcc(writer, preCacheData, tryStat(fs, path)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}
