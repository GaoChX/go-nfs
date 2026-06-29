package nfs

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// writeStability is the level of durability requested with the write
type writeStability uint32

const (
	unstable writeStability = 0
	dataSync writeStability = 1
	fileSync writeStability = 2
)

type writeArgs struct {
	Handle []byte
	Offset uint64
	Count  uint32
	How    uint32
	Data   []byte
}

func onWrite(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = wccDataErrorFormatter
	var req writeArgs
	if err := xdr.Read(w.req.Body, &req); err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}

	fs, path, err := userHandle.FromHandle(ctx, req.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	if !billy.CapabilityCheck(fs, billy.WriteCapability) {
		return &NFSStatusError{NFSStatusROFS, os.ErrPermission}
	}
	if len(req.Data) > math.MaxInt32 || req.Count > math.MaxInt32 {
		return &NFSStatusError{NFSStatusFBig, os.ErrInvalid}
	}
	if req.How != uint32(unstable) && req.How != uint32(dataSync) && req.How != uint32(fileSync) {
		return &NFSStatusError{NFSStatusInval, os.ErrInvalid}
	}

	fullPath := fs.Join(path...)

	end := req.Count
	if len(req.Data) < int(end) {
		end = uint32(len(req.Data))
	}

	// All writes reuse a per-file-handle cached open fd, so a run of writes to
	// the same file pays one open instead of open+write+close per request, and
	// the wcc attributes come from an fstat on that fd (a direct syscall) rather
	// than the backing filesystem's potentially serialized path stat.
	//
	// The only difference by stability: unstable writes return without flushing
	// (durability deferred to COMMIT), while dataSync/fileSync writes Sync the fd
	// before replying so their durability contract is honored immediately. Both
	// keep the fd open.
	var (
		writtenCount int
		stability    = writeStability(req.How)
		preOpCache   *FileCacheAttribute
		postOp       *FileAttribute
	)

	// Fast path for the pre-op wcc: if the file already has a cached handle,
	// fstat it instead of a worker-serialized path stat. On the first write (no
	// cached handle yet) fall back to a path stat, which also validates
	// existence and file type before opening.
	cached := w.writeHandleCache().get(string(req.Handle))
	if cached != nil {
		if fi, ok := cached.stat(); ok {
			preOpCache = ToFileAttribute(fi, fullPath).AsCache()
		}
	}
	if preOpCache == nil {
		info, serr := statRegularFile(fs, fullPath)
		if serr != nil {
			return serr
		}
		preOpCache = ToFileAttribute(info, fullPath).AsCache()
	}

	var h *cachedHandle
	writtenCount, h, err = cachedWrite(w, fs, req.Handle, fullPath, req.Data[:end], int64(req.Offset), cached)
	if err != nil {
		Log.Errorf("Error writing: %v", err)
		return &NFSStatusError{statusFromWriteError(err), err}
	}

	// For dataSync/fileSync, flush before replying. Sync keeps the fd open;
	// closeFlush is the fallback when the backing file cannot Sync. Either way
	// we report fileSync, which is a valid upgrade of the requested stability.
	if req.How != uint32(unstable) {
		synced, serr := h.sync()
		switch {
		case errors.Is(serr, errHandleClosed):
			// A concurrent drop/eviction (SETATTR/REMOVE/RENAME on another
			// connection) closed the fd between our write and this sync. That close
			// also flushed our write, but it may have failed (ENOSPC/EIO), which the
			// drop records on the mount-wide tracker. A stable WRITE must honor
			// durability now — its client will not send a COMMIT to learn of a
			// deferred failure — so wait for the in-flight close and surface any
			// recorded loss instead of assuming the data is durable.
			if cerr := w.Server.commitsTracker().wait(string(req.Handle)); cerr != nil {
				Log.Errorf("Error flushing on concurrent close: %v", cerr)
				return &NFSStatusError{statusFromWriteError(cerr), cerr}
			}
		case serr != nil:
			Log.Errorf("Error syncing: %v", serr)
			return &NFSStatusError{statusFromWriteError(serr), serr}
		case !synced:
			// No Sync() capability: close to force a flush, recording any lost-dirty
			// failure on the tracker (see flushHandle) before reporting it to this
			// client. Close the handle removeAndTrack actually detached, not the
			// possibly-replaced h, and use evictFlush so only a still-dirty close
			// failure counts as a loss.
			if dropped, done := w.writeHandleCache().removeAndTrack(string(req.Handle)); dropped != nil {
				lost, cerr := dropped.evictFlush()
				w.Server.commitsTracker().finish(string(req.Handle), done, lostErr(lost, cerr))
				if cerr != nil {
					Log.Errorf("Error flushing on close: %v", cerr)
					return &NFSStatusError{statusFromWriteError(cerr), cerr}
				}
			}
			// The handle we synced may have been concurrently closed (its failed
			// close recorded on the tracker) and a fresh handle cached for the same
			// key before removeAndTrack — so the handle we just closed above is not
			// the one that held our bytes, and its clean close says nothing about
			// their durability. A stable WRITE is the durability checkpoint (its
			// dataSync/fileSync client will not send a COMMIT to learn of a deferred
			// failure), so always consult the tracker and surface any recorded loss
			// before acknowledging durability. wait blocks for any in-flight close.
			if cerr := w.Server.commitsTracker().wait(string(req.Handle)); cerr != nil {
				Log.Errorf("Error flushing on concurrent close: %v", cerr)
				return &NFSStatusError{statusFromWriteError(cerr), cerr}
			}
		}
		stability = fileSync
	}

	// Build post-op wcc from an fstat on the open fd when possible.
	if fi, ok := h.stat(); ok {
		postOp = ToFileAttribute(fi, fullPath)
	} else {
		postOp = tryStat(fs, path)
	}

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := WriteWcc(writer, preOpCache, postOp); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := xdr.Write(writer, uint32(writtenCount)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := xdr.Write(writer, stability); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := xdr.Write(writer, w.Server.currentWriteVerifier()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}

// statRegularFile path-stats fullPath and validates it exists and is a regular
// file, mapping errors to NFS status errors. Used for the pre-op wcc and to
// reject writes to non-regular files.
func statRegularFile(fs billy.Filesystem, fullPath string) (os.FileInfo, error) {
	info, err := fs.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &NFSStatusError{NFSStatusNoEnt, err}
		}
		return nil, &NFSStatusError{NFSStatusAccess, err}
	}
	if !info.Mode().IsRegular() {
		return nil, &NFSStatusError{NFSStatusInval, os.ErrInvalid}
	}

	return info, nil
}

// cachedWrite writes through a per-file-handle cached, kept-open fd. Unstable
// writes are made durable later by onCommit (or the idle sweeper); dataSync/
// fileSync writes are flushed by the caller via the returned handle. It returns
// the handle so the caller can fstat it for post-op attributes.
//
// cachedHint is a handle the caller already looked up for this key (e.g. for the
// pre-op wcc fstat); when non-nil it seeds the first attempt so the common
// already-cached path takes the writeCache lock once instead of twice per WRITE.
// It is only a hint: if it was closed by a concurrent eviction, writeAt reports
// errHandleClosed and the retry re-fetches under the lock exactly as a cold call
// would, so correctness does not depend on the hint being live.
func cachedWrite(w *response, fs billy.Filesystem, handle []byte, fullPath string, data []byte, offset int64, cachedHint *cachedHandle) (int, *cachedHandle, error) {
	cache := w.writeHandleCache()
	key := string(handle)

	// Retry once: the cached handle may be closed by eviction between lookup
	// and write.
	for attempt := 0; attempt < 2; attempt++ {
		h := cachedHint
		cachedHint = nil // only valid for the first attempt; re-fetch on retry
		if h == nil {
			h = cache.get(key)
		}
		if h == nil {
			// Sample the invalidation generation BEFORE opening: if a concurrent
			// SETATTR/REMOVE/RENAME/CREATE drops the handle (bumping gen) while we
			// are mid-open, putIfFresh refuses to cache this now-stale fd — opened
			// under the old mode/size — so later writes cannot reuse it to bypass a
			// chmod or re-extend past a truncate.
			gen := cache.sampleGen()
			// O_RDWR on an existing file; perm is ignored without O_CREATE.
			file, err := fs.OpenFile(fullPath, os.O_RDWR, 0)
			if err != nil {
				return 0, nil, err
			}
			h = &cachedHandle{file: file, lastUsed: time.Now()}
			if !cache.putIfFresh(key, h, gen) {
				// An invalidation raced our open, or another goroutine published
				// first. Close our stale fd and retry: the next iteration either
				// reuses the winner's handle or reopens under the new attributes.
				_ = h.closeFlush()
				continue
			}
		}

		n, err := h.writeAt(data, offset)
		if errors.Is(err, errHandleClosed) {
			// Our handle was closed by a concurrent eviction/invalidation. Drop it
			// and retry — but only if it is still the cached handle for key: another
			// WRITE may have published a fresh fd in the meantime, and removing that
			// would leave its unstable writes unreachable to COMMIT/drain.
			cache.removeIf(key, h)
			continue
		}

		return n, h, err
	}

	return 0, nil, errHandleClosed
}
