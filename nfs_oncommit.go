package nfs

import (
	"bytes"
	"context"
	"os"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// onCommit flushes data previously written with unstable stability to the
// backing store. NFSv3 clients send COMMIT after a batch of unstable writes to
// make them durable; we flush the cached handle for the file (if any) so those
// writes reach stable storage before replying.
func onCommit(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = wccDataErrorFormatter
	handle, err := xdr.ReadOpaque(w.req.Body)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	// The conn will drain the unread offset and count arguments.

	fs, path, err := userHandle.FromHandle(ctx, handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	if !billy.CapabilityCheck(fs, billy.WriteCapability) {
		return &NFSStatusError{NFSStatusServerFault, os.ErrPermission}
	}

	// Flush the file's unstable writes to the backing store. With nconnect the
	// writes for this file may have been cached on a different connection than
	// the one receiving COMMIT, so flush every connection's cached write handle
	// for this handle, not just the local one.
	found, ferr := w.Server.flushHandleAll(handle)
	if ferr != nil {
		return &NFSStatusError{statusFromWriteError(ferr), ferr}
	}

	// A handle for this file may have been evicted (LRU/idle/drop) or drained on
	// a disconnecting connection without a client to report a close-flush failure
	// to. The mount-wide commit tracker records such failures (and marks evictions
	// whose flush is still in flight); consult it before trusting the path-level
	// fallback. wait blocks for any in-flight eviction so its result is observed
	// rather than raced past, and returns a non-nil error if acknowledged unstable
	// data was lost.
	if cerr := w.Server.commitsTracker().wait(string(handle)); cerr != nil {
		return &NFSStatusError{statusFromWriteError(cerr), cerr}
	}

	// Only when no connection held a cached handle do we fall back to opening the
	// path and fsyncing it directly (covers an already-evicted/flushed handle or
	// a no-op COMMIT).
	if !found {
		if err := commitByPath(fs, fs.Join(path...)); err != nil {
			return &NFSStatusError{statusFromWriteError(err), err}
		}
	}

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return err
	}

	// no pre-op cache data.
	if err := xdr.Write(writer, uint32(0)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WritePostOpAttrs(writer, tryStat(fs, path)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	// write the 8 bytes of write verification.
	if err := xdr.Write(writer, w.Server.currentWriteVerifier()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}

// commitByPath makes a file's unstable writes durable when no connection has a
// cached write handle for it — e.g. nconnect spread the writes and COMMIT
// across different connections, the dirty handle was already evicted and
// flushed, or the client is committing a file with nothing pending.
//
// It opens read-only: fsync requires no write permission, and there is no
// cached dirty handle to flush here, so a read-only open both works for
// read-only files / files whose write permission was chmod'd away and still
// triggers an inode-level fsync (which flushes writes made through any other
// fd). When the backing file exposes Sync we fsync it; otherwise close is a
// best effort. A permission error opening for this best-effort flush is treated
// as a no-op success: there is no pending data of ours to lose, and COMMIT on a
// file with nothing cached was a successful no-op before the handle cache.
func commitByPath(fs billy.Filesystem, fullPath string) error {
	f, err := fs.OpenFile(fullPath, os.O_RDONLY, 0)
	if err != nil {
		if os.IsNotExist(err) {
			// Nothing written/exists to commit.
			return nil
		}
		if os.IsPermission(err) {
			// Can't open even read-only; nothing of ours is pending, so a COMMIT
			// here is a no-op rather than a durability failure.
			return nil
		}
		return err
	}
	if s, ok := f.(syncer); ok {
		if serr := s.Sync(); serr != nil {
			_ = f.Close()
			return serr
		}
		return f.Close()
	}

	// No inode-level flush available; close is the best we can do. Report
	// success: the common callers here are already-clean files (evicted/flushed
	// handle, or a no-op COMMIT).
	return f.Close()
}
