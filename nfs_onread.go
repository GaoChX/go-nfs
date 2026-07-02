package nfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

type nfsReadArgs struct {
	Handle []byte
	Offset uint64
	Count  uint32
}

type nfsReadResponse struct {
	Count uint32
	EOF   uint32
	Data  []byte
}

// MaxRead is the largest READ the server will serve in one reply, and the rtmax
// it advertises in FSInfo (see onFSInfo, which reports MaxRead). It doubles as a
// per-request allocation bound: onRead allocates a buffer of the (clamped) READ
// count, and with concurrent per-connection workers up to MaxConcurrentRequests
// such buffers may be live at once, so this must stay aligned with the
// advertised rtmax — a larger MaxRead than advertised lets a client force
// oversized allocations on every worker. 1 MiB matches the Linux client rsize
// cap and the advertised wtmax/rtmax.
const MaxRead = 1 << 20

// CheckRead is a size where - if a request to read is larger than this,
// the server will stat the file to learn it's actual size before allocating
// a buffer to read into.
const CheckRead = 1 << 15

func onRead(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = opAttrErrorFormatter
	var obj nfsReadArgs
	err := xdr.Read(w.req.Body, &obj)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	fs, path, err := userHandle.FromHandle(ctx, obj.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}

	// Read-after-unstable-write coherence: a prior unstable WRITE may have left
	// dirty bytes only in the cached write fd. On backends whose billy.File
	// buffers until Sync/Close, a READ through the separate read-only fd would
	// miss them, so flush any dirty write handle first (broadcast, since nconnect
	// may have cached the write on another connection). A no-op cache lookup when
	// nothing is dirty.
	if ferr := w.Server.flushDirtyHandleAll(obj.Handle); ferr != nil {
		return &NFSStatusError{statusFromWriteError(ferr), ferr}
	}

	// Reuse a cached read-only fd across READs to the same file so a run of
	// reads pays one open instead of open+read+close per request, and size/attrs
	// come from an fstat on the fd rather than a worker-serialized path stat.
	h, cached, ferr := readHandleFor(w, fs, obj.Handle, fs.Join(path...))
	if ferr != nil {
		return ferr
	}
	// If the handle is not owned by the cache (a concurrent invalidation refused
	// its publish), close it after this read so the private fd is not leaked. The
	// closure reads the current h/cached, which the errHandleClosed retry below
	// may replace, so it always closes the handle actually used and only when it
	// is uncached.
	defer func() {
		if !cached {
			_ = h.close()
		}
	}()

	resp := nfsReadResponse{}
	setEOF := false

	fullPath := fs.Join(path...)
	var info os.FileInfo
	if fi, ok := h.stat(); ok {
		info = fi
	} else {
		info, err = fs.Stat(fullPath)
		if err != nil {
			return &NFSStatusError{NFSStatusAccess, err}
		}
	}
	size := info.Size()
	if int64(obj.Offset) >= size {
		obj.Count = 0
		setEOF = true
	} else if size-int64(obj.Offset) <= int64(obj.Count) {
		obj.Count = uint32(uint64(size) - obj.Offset)
		setEOF = true
	}
	if obj.Count > MaxRead {
		obj.Count = MaxRead
	}
	resp.Data = make([]byte, obj.Count)
	// todo: multiple reads if size isn't full
	cnt, err := h.readAt(resp.Data, int64(obj.Offset))
	if errors.Is(err, errHandleClosed) {
		// Raced with eviction; drop and retry once with a fresh handle. Only remove
		// h if it is still the cached handle: another READ may have cached a fresh
		// fd for this key, and an unconditional remove would orphan that fd (no
		// cache entry owns it, so neither drain nor idle sweep closes it).
		w.readHandleCache().removeIf(string(obj.Handle), h)
		h, cached, ferr = readHandleFor(w, fs, obj.Handle, fs.Join(path...))
		if ferr != nil {
			return ferr
		}
		cnt, err = h.readAt(resp.Data, int64(obj.Offset))
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return &NFSStatusError{NFSStatusIO, err}
	}
	resp.Count = uint32(cnt)
	resp.Data = resp.Data[:resp.Count]
	if errors.Is(err, io.EOF) || setEOF {
		resp.EOF = 1
	}

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WritePostOpAttrs(writer, ToFileAttribute(info, fullPath)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := xdr.Write(writer, resp); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}

// readHandleFor returns an open read-only handle for the file, preferring a
// cached one. The returned cached flag reports whether the handle is owned by
// the cache (drain/idle sweep will close it) or is a private, uncached fd the
// caller must close after the read.
//
// Caching is best-effort: if a concurrent invalidation refuses our publish
// (putIfFresh false), we do NOT fail the READ — the freshly-opened fd is valid
// to serve this one read (a cacheless server would open-read-close exactly the
// same way, and concurrent READ vs SETATTR/etc. ordering is unspecified). We
// only decline to cache it, so later reads reopen rather than persistently
// reusing an fd opened under now-stale attributes. This matters because the
// invalidation generation is cache-wide: an unrelated WRITE's read-cache drop
// (dropReadHandleAll) bumps it too, and must not turn a valid cold READ into a
// spurious ESTALE.
func readHandleFor(w *response, fs billy.Filesystem, handle []byte, fullPath string) (h *readHandle, cached bool, err error) {
	cache := w.readHandleCache()
	key := string(handle)

	// Retry once to reuse a winner's freshly-cached handle: a concurrent open for
	// the same key may publish between our get miss and our putIfFresh refusal.
	for attempt := 0; attempt < 2; attempt++ {
		if h := cache.get(key); h != nil {
			return h, true, nil
		}

		// Sample this key's invalidation generation BEFORE opening: if a concurrent
		// SETATTR/REMOVE/RENAME/CREATE drops the handle (bumping the key's gen) while
		// we are mid-open, putIfFresh refuses to cache this now-stale read-only fd —
		// opened under the old mode/size — so later reads cannot serve
		// pre-chmod/pre-truncate data through it. Per-key gen, so an unrelated file's
		// invalidation does not refuse this open. sampleGen must be paired with
		// exactly one resolveGen on every exit below.
		gen := cache.sampleGen(key)
		fh, oerr := fs.Open(fullPath)
		if oerr != nil {
			cache.resolveGen(key)
			if os.IsNotExist(oerr) {
				return nil, false, &NFSStatusError{NFSStatusNoEnt, oerr}
			}
			return nil, false, &NFSStatusError{NFSStatusAccess, oerr}
		}
		nh := &readHandle{file: fh}
		nh.lastUsed.Store(time.Now().UnixNano())
		published := cache.putIfFresh(key, nh, gen)
		cache.resolveGen(key)
		if published {
			return nh, true, nil
		}
		// Publish refused (an invalidation raced our open, or another goroutine
		// published first). On the first attempt, loop to reuse a winner's handle.
		// On the last attempt, serve this read from the uncached fd rather than
		// failing: it is a valid open fd, we just decline to cache it. The caller
		// closes it after the read.
		if attempt == 1 {
			return nh, false, nil
		}
		_ = nh.close()
	}

	// Unreachable: the loop returns on both attempts.
	return nil, false, &NFSStatusError{NFSStatusStale, errHandleClosed}
}
