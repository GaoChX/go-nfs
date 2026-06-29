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
	writeStart := time.Now()
	defer func() {
		writeProfile.writes.Add(1)
		writeProfile.totalNs.Add(time.Since(writeStart).Nanoseconds())
	}()
	w.errorFmt = wccDataErrorFormatter
	var req writeArgs
	decodeStart := time.Now()
	derr := xdr.Read(w.req.Body, &req)
	writeProfile.decodeNs.Add(time.Since(decodeStart).Nanoseconds())
	if derr != nil {
		return &NFSStatusError{NFSStatusInval, derr}
	}

	fhStart := time.Now()
	fs, path, err := userHandle.FromHandle(ctx, req.Handle)
	writeProfile.fromHandleNs.Add(time.Since(fhStart).Nanoseconds())
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	var (
		fullPath string
		end      uint32
	)
	validateStart := time.Now()
	validateErr := func() error {
		if !billy.CapabilityCheck(fs, billy.WriteCapability) {
			return &NFSStatusError{NFSStatusROFS, os.ErrPermission}
		}
		if len(req.Data) > math.MaxInt32 || req.Count > math.MaxInt32 {
			return &NFSStatusError{NFSStatusFBig, os.ErrInvalid}
		}
		if req.How != uint32(unstable) && req.How != uint32(dataSync) && req.How != uint32(fileSync) {
			return &NFSStatusError{NFSStatusInval, os.ErrInvalid}
		}

		fullPath = fs.Join(path...)
		end = req.Count
		if len(req.Data) < int(end) {
			end = uint32(len(req.Data))
		}

		return nil
	}()
	writeProfile.validateNs.Add(time.Since(validateStart).Nanoseconds())
	if validateErr != nil {
		return validateErr
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
	writeProfile.recordStability(stability)

	// Pre-op wcc attributes are an optional optimization in NFSv3 (the wcc_attr
	// "before" field MAY be omitted). Sample them only from an already-open
	// cached fd, which fstats the fd directly (cheap, no path resolution). On a
	// cold write (no cached handle yet) we deliberately SKIP the pre-op attrs
	// rather than do a path Stat: on the e2b chroot backend a path Stat is
	// funneled through a single-threaded mount-namespace executor (mountNS.Do),
	// which serializes every WRITE and is the dominant cost under load. Existence
	// and writability are still enforced — cachedWrite opens the file O_RDWR and
	// fails cleanly if it is missing or not writable — and regular-file type is
	// re-checked from the opened fd below.
	preStatStart := time.Now()
	cached := w.writeHandleCache().get(string(req.Handle))
	if cached != nil {
		if fi, ok := cached.stat(); ok {
			preOpCache = ToFileAttribute(fi, fullPath).AsCache()
		}
	}
	writeProfile.preStatNs.Add(time.Since(preStatStart).Nanoseconds())

	var h *cachedHandle
	cwStart := time.Now()
	writtenCount, h, err = cachedWrite(w, fs, req.Handle, fullPath, req.Data[:end], int64(req.Offset), cached)
	writeProfile.cachedWriteNs.Add(time.Since(cwStart).Nanoseconds())
	if err != nil {
		Log.Errorf("Error writing: %v", err)
		return &NFSStatusError{statusFromWriteError(err), err}
	}

	// For dataSync/fileSync, flush before replying. Sync keeps the fd open;
	// closeFlush is the fallback when the backing file cannot Sync. Either way
	// we report fileSync, which is a valid upgrade of the requested stability.
	if req.How != uint32(unstable) {
		stableErr := func() error {
			stableStart := time.Now()
			defer func() {
				writeProfile.stableWriteNs.Add(time.Since(stableStart).Nanoseconds())
			}()
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

			return nil
		}()
		if stableErr != nil {
			return stableErr
		}
		stability = fileSync
	}

	// Build post-op wcc from an fstat on the open fd when possible. This fstat
	// also serves as the regular-file-type guard that the (now skipped) pre-op
	// path Stat used to provide: a WRITE to a directory/device/etc. is rejected
	// here from the fd's mode, without a path Stat.
	postStatStart := time.Now()
	if fi, ok := h.stat(); ok {
		if !fi.Mode().IsRegular() {
			writeProfile.postStatNs.Add(time.Since(postStatStart).Nanoseconds())
			return &NFSStatusError{NFSStatusInval, os.ErrInvalid}
		}
		postOp = ToFileAttribute(fi, fullPath)
	} else {
		postOp = tryStat(fs, path)
	}
	writeProfile.postStatNs.Add(time.Since(postStatStart).Nanoseconds())

	replyStart := time.Now()
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
	writeProfile.replyNs.Add(time.Since(replyStart).Nanoseconds())
	return nil
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
		h := cache.getCurrent(key, cachedHint)
		cachedHint = nil // only valid for the first attempt; re-fetch on retry
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
