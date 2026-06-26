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

// MaxRead is the advertised largest buffer the server is willing to read
const MaxRead = 1 << 24

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
	h, ferr := readHandleFor(w, fs, obj.Handle, fs.Join(path...))
	if ferr != nil {
		return ferr
	}

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
		h, ferr = readHandleFor(w, fs, obj.Handle, fs.Join(path...))
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

// readHandleFor returns a cached read-only handle for the file, opening and
// caching one on a miss.
func readHandleFor(w *response, fs billy.Filesystem, handle []byte, fullPath string) (*readHandle, error) {
	cache := w.readHandleCache()
	key := string(handle)

	// Retry once: a concurrent invalidation can refuse our publish, in which case
	// we reopen (or reuse the winner's handle) on the next iteration.
	for attempt := 0; attempt < 2; attempt++ {
		if h := cache.get(key); h != nil {
			return h, nil
		}

		// Sample the invalidation generation BEFORE opening: if a concurrent
		// SETATTR/REMOVE/RENAME/CREATE drops the handle (bumping gen) while we are
		// mid-open, putIfFresh refuses to cache this now-stale read-only fd —
		// opened under the old mode/size — so later reads cannot serve
		// pre-chmod/pre-truncate data through it.
		gen := cache.sampleGen()
		fh, err := fs.Open(fullPath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, &NFSStatusError{NFSStatusNoEnt, err}
			}
			return nil, &NFSStatusError{NFSStatusAccess, err}
		}
		h := &readHandle{file: fh}
		h.lastUsed.Store(time.Now().UnixNano())
		if cache.putIfFresh(key, h, gen) {
			return h, nil
		}
		// An invalidation raced our open, or another goroutine published first.
		// Close our stale fd and retry: the next iteration reuses the winner's
		// handle or reopens under the new attributes.
		_ = h.close()
	}

	return nil, &NFSStatusError{NFSStatusStale, errHandleClosed}
}
