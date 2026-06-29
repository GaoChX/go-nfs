package nfs

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	xdr2 "github.com/rasky/go-xdr/xdr2"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

var (
	// ErrInputInvalid is returned when input cannot be parsed
	ErrInputInvalid = errors.New("invalid input")
	// ErrAlreadySent is returned when writing a header/status multiple times
	ErrAlreadySent = errors.New("response already started")
)

// ResponseCode is a combination of accept_stat and reject_stat.
type ResponseCode uint32

// ResponseCode Codes
const (
	ResponseCodeSuccess ResponseCode = iota
	ResponseCodeProgUnavailable
	ResponseCodeProcUnavailable
	ResponseCodeGarbageArgs
	ResponseCodeSystemErr
	ResponseCodeRPCMismatch
	ResponseCodeAuthError
)

type conn struct {
	*Server
	writeSerializer chan []byte
	net.Conn

	// Per-connection handle caches. An NFS mount maps to a connection, so these
	// are drained when the client disconnects (see serve), releasing fds
	// promptly instead of waiting for the idle sweeper. cacheMu guards the
	// pointers because broadcast invalidation (Server.dropHandleAll) reads them
	// from other connections' goroutines while this connection may be lazily
	// creating them on its first READ/WRITE.
	cacheMu sync.Mutex
	wc      *writeCache
	rc      *readCache
}

// writeHandleCache returns the connection's unstable-write handle cache,
// creating it on first use.
func (c *conn) writeHandleCache() *writeCache {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	if c.wc == nil {
		c.wc = newWriteCache(c.Server.commitsTracker())
	}

	return c.wc
}

// readHandleCache returns the connection's read handle cache, creating it on
// first use.
func (c *conn) readHandleCache() *readCache {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	if c.rc == nil {
		c.rc = newReadCache()
	}

	return c.rc
}

// dropHandle flushes/closes any cached write and read handles for the given NFS
// file handle. Safe to call from another connection's goroutine (broadcast
// invalidation), so the cache pointers are read under cacheMu.
func (c *conn) dropHandle(handle []byte) error {
	c.cacheMu.Lock()
	wc, rc := c.wc, c.rc
	c.cacheMu.Unlock()

	var err error
	if wc != nil {
		if h, done := wc.invalidate(string(handle)); h != nil {
			// Use evictFlush, not closeFlush: like LRU/idle eviction this drop has
			// no client to return a flush error to. If a still-dirty handle fails
			// to close (ENOSPC/EIO), record it on the mount-wide tracker so a later
			// COMMIT for this file (possibly on a peer connection) surfaces it
			// instead of falsely succeeding via commitByPath.
			lost, cerr := h.evictFlush()
			wc.commits.finish(string(handle), done, lostErr(lost, cerr))
			err = cerr
		}
	}
	if rc != nil {
		if h := rc.invalidate(string(handle)); h != nil {
			if cerr := h.close(); cerr != nil && err == nil {
				err = cerr
			}
		}
	}

	return err
}

// flushHandle flushes (without closing) any cached write handle for the given
// NFS handle so its unstable writes reach stable storage, keeping the fd open
// for further writes. Falls back to close (which also flushes) when the backing
// file cannot Sync. Safe to call from another connection's goroutine (COMMIT
// broadcast), so the cache pointer is read under cacheMu. Returns whether a
// cached handle was found, so the caller can decide whether a path-level fsync
// fallback is still needed.
func (c *conn) flushHandle(handle []byte) (bool, error) {
	c.cacheMu.Lock()
	wc := c.wc
	c.cacheMu.Unlock()

	if wc == nil {
		return false, nil
	}

	h := wc.get(string(handle))
	if h == nil {
		return false, nil
	}

	synced, err := h.sync()
	if err != nil {
		// A handle closed by a concurrent eviction was already flushed on close,
		// so its writes are durable; only surface real sync failures.
		if errors.Is(err, errHandleClosed) {
			return true, nil
		}
		return true, err
	}
	if !synced {
		// No Sync() capability: close to force a flush, then drop so the next
		// write reopens. removeAndTrack publishes a pending marker before the
		// handle leaves the cache, and finish records a lost-dirty close failure on
		// the mount-wide tracker: this COMMIT returns the error now, but the handle
		// is gone, so a retried COMMIT (or a retry after a lost error response) must
		// still surface the loss via the tracker instead of falsely succeeding
		// through commitByPath. Close the handle removeAndTrack actually detached:
		// a concurrent eviction+rewrite may have replaced the one we synced, and
		// closing the stale h would leave the live handle open, dirty, and
		// unreachable. evictFlush records a loss only when the closed handle was
		// still dirty.
		if dropped, done := wc.removeAndTrack(string(handle)); dropped != nil {
			lost, cerr := dropped.evictFlush()
			c.Server.commitsTracker().finish(string(handle), done, lostErr(lost, cerr))
			if cerr != nil {
				return true, cerr
			}
		}
	}

	return true, nil
}

// flushDirtyHandle flushes this connection's cached write handle for the given
// NFS handle only if it holds unflushed unstable writes, so a READ served from
// a separate read-only fd observes them on backends that buffer until Sync/
// Close. Safe to call from another connection's goroutine (READ broadcast), so
// the cache pointer is read under cacheMu. Returns whether a dirty handle was
// found and flushed.
func (c *conn) flushDirtyHandle(handle []byte) (bool, error) {
	c.cacheMu.Lock()
	wc := c.wc
	c.cacheMu.Unlock()

	if wc == nil {
		return false, nil
	}

	return wc.flushDirty(string(handle))
}

// drainCaches flushes and closes all cached handles for this connection.
func (c *conn) drainCaches() {
	c.cacheMu.Lock()
	wc, rc := c.wc, c.rc
	c.cacheMu.Unlock()

	if wc != nil {
		wc.Close()
	}
	if rc != nil {
		rc.Close()
	}
}

func (c *conn) serve(ctx context.Context) {
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if hook := c.OnConnect; hook != nil {
		connCtx, c.Conn = hook(connCtx, c.Conn)
	}

	c.registerConn(c)

	// inFlight tracks handler goroutines so teardown can wait for them before
	// draining the caches (a worker may still hold a cached read/write fd).
	var inFlight sync.WaitGroup
	defer func() {
		// Stop the serializer and unblock any worker parked on finish, then wait
		// for every in-flight handler to return BEFORE draining: a worker may still
		// be touching this connection's handle caches, and closing those fds out
		// from under an active request would corrupt it.
		cancel()
		inFlight.Wait()
		// Drain (close fds) before unregistering so a concurrent broadcast
		// invalidation on another connection still visits this one while its
		// cached fds are open; unregistering first would let a snapshot miss it
		// and leave an fd open (failing Windows-like remove/rename).
		c.drainCaches()
		c.unregisterConn(c)
		if hook := c.OnDisconnect; hook != nil {
			hook(connCtx, c.Conn)
		}
	}()

	// Bound how many requests this connection processes concurrently. Reading is
	// serial — one reader per socket pulls each RPC record off the wire in order —
	// but handling (the disk IO) fans out to up to `limit` goroutines, so a run of
	// pipelined requests on an nconnect mount no longer waits for each
	// predecessor's IO to complete. This mirrors knfsd's pool of nfsd threads and
	// nfs-ganesha's worker pool; replies are matched by XID and may return out of
	// order, which the protocol allows.
	limit := c.Server.MaxConcurrentRequests
	if limit <= 0 {
		limit = DefaultMaxConcurrentRequests
	}
	sem := make(chan struct{}, limit)

	// Buffer the serializer to the concurrency limit so completed handlers can
	// hand off their reply without blocking on the single socket writer.
	c.writeSerializer = make(chan []byte, limit)
	go c.serializeWrites(connCtx)

	bio := bufio.NewReader(c.Conn)
	for {
		w, err := c.readRequestHeader(connCtx, bio)
		if err != nil {
			if err == io.EOF {
				// Clean close.
				c.Close()
			}
			return
		}
		Log.Tracef("request: %v", w.req)

		// Acquire a worker slot (or bail out if the connection is tearing down).
		select {
		case sem <- struct{}{}:
		case <-connCtx.Done():
			return
		}
		inFlight.Add(1)
		go func(w *response) {
			defer inFlight.Done()
			defer func() { <-sem }()
			// The reply is built into w.writer (independent of the record buffer), so
			// once handle returns nothing aliases req.Body and the buffer can go back
			// to the pool for the next request to reuse.
			defer w.release()

			if err := c.handle(connCtx, w); err != nil {
				Log.Errorf("error handling req: %v", err)
				// Failure at a level needing the connection closed. Cancel to stop the
				// serializer and Close to unblock the serial reader.
				cancel()
				c.Close()
				return
			}
			if err := w.finish(connCtx); err != nil {
				Log.Errorf("error sending response: %v", err)
				cancel()
				c.Close()
			}
		}(w)
	}
}

func (c *conn) serializeWrites(ctx context.Context) {
	// todo: maybe don't need the extra buffer
	writer := bufio.NewWriter(c.Conn)
	var fragmentBuf [4]byte

	// writeMsg frames and writes one reply into the buffered writer (no flush).
	writeMsg := func(msg []byte) bool {
		fragmentInt := uint32(len(msg)) | (1 << 31)
		binary.BigEndian.PutUint32(fragmentBuf[:], fragmentInt)
		if n, err := writer.Write(fragmentBuf[:]); n < 4 || err != nil {
			return false
		}
		n, err := writer.Write(msg)
		if err != nil {
			return false
		}
		if n < len(msg) {
			panic("todo: ensure writes complete fully.")
		}
		return true
	}

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-c.writeSerializer:
			if !ok {
				return
			}
			if !writeMsg(msg) {
				return
			}
			// Coalesce flushes: with concurrent handlers many replies queue up at
			// once, so drain everything already buffered in the channel and flush a
			// single time rather than paying a write syscall per reply. When idle the
			// channel is empty after one message, so this still flushes immediately.
			drained := false
			for !drained {
				select {
				case msg, ok := <-c.writeSerializer:
					if !ok {
						drained = true
						break
					}
					if !writeMsg(msg) {
						return
					}
				default:
					drained = true
				}
			}
			if err := writer.Flush(); err != nil {
				return
			}
		}
	}
}

// Handle a request. errors from this method indicate a failure to read or
// write on the network stream, and trigger a disconnection of the connection.
func (c *conn) handle(ctx context.Context, w *response) error {
	handler := c.Server.handlerFor(w.req.Header.Prog, w.req.Header.Proc)
	if handler == nil {
		Log.Debugf("No handler for %d.%d", w.req.Header.Prog, w.req.Header.Proc)
		if err := w.drain(ctx); err != nil {
			return err
		}
		return c.err(ctx, w, &ResponseCodeProcUnavailableError{})
	}
	appError := handler(ctx, w, c.Server.Handler)
	if drainErr := w.drain(ctx); drainErr != nil {
		return drainErr
	}
	if appError != nil && !w.responded {
		if err := c.err(ctx, w, appError); err != nil {
			return err
		}
	}
	if !w.responded {
		Log.Errorf("Handler did not indicate response status via writing or erroring")
		if err := c.err(ctx, w, &ResponseCodeSystemError{}); err != nil {
			return err
		}
	}
	return nil
}

func (c *conn) err(ctx context.Context, w *response, err error) error {
	select {
	case <-ctx.Done():
		return nil
	default:
	}

	if w.err == nil {
		w.err = err
	}

	if w.responded {
		return nil
	}

	rpcErr := w.errorFmt(err)
	if writeErr := w.writeHeader(rpcErr.Code()); writeErr != nil {
		return writeErr
	}

	body, _ := rpcErr.MarshalBinary()
	return w.Write(body)
}

type request struct {
	xid uint32
	rpc.Header
	Body io.Reader
}

func (r *request) String() string {
	if r.Header.Prog == nfsServiceID {
		return fmt.Sprintf("RPC #%d (nfs.%s)", r.xid, NFSProcedure(r.Header.Proc))
	} else if r.Header.Prog == mountServiceID {
		return fmt.Sprintf("RPC #%d (mount.%s)", r.xid, MountProcedure(r.Header.Proc))
	}
	return fmt.Sprintf("RPC #%d (%d.%d)", r.xid, r.Header.Prog, r.Header.Proc)
}

type response struct {
	*conn
	writer    *bytes.Buffer
	responded bool
	err       error
	errorFmt  func(error) RPCError
	req       *request
	// recordBuf is the pooled buffer backing req.Body, returned to the pool by
	// release once handling completes (the reply is built in writer, which is
	// independent, so the record bytes are no longer needed). nil for responses
	// constructed in tests.
	recordBuf *[]byte
}

// release returns the request's pooled record buffer. Safe to call once, after
// the handler has finished consuming req.Body.
func (w *response) release() {
	if w.recordBuf != nil {
		putRecordBuf(w.recordBuf)
		w.recordBuf = nil
	}
}

func (w *response) writeXdrHeader() error {
	err := xdr.Write(w.writer, &w.req.xid)
	if err != nil {
		return err
	}
	respType := uint32(1)
	err = xdr.Write(w.writer, &respType)
	if err != nil {
		return err
	}
	return nil
}

func (w *response) writeHeader(code ResponseCode) error {
	if w.responded {
		return ErrAlreadySent
	}
	w.responded = true
	if err := w.writeXdrHeader(); err != nil {
		return err
	}

	status := rpc.MsgAccepted
	if code == ResponseCodeAuthError || code == ResponseCodeRPCMismatch {
		status = rpc.MsgDenied
	}

	err := xdr.Write(w.writer, &status)
	if err != nil {
		return err
	}

	if status == rpc.MsgAccepted {
		// Write opaque_auth header.
		err = xdr.Write(w.writer, &rpc.AuthNull)
		if err != nil {
			return err
		}
	}

	return xdr.Write(w.writer, &code)
}

// Write a response to an xdr message
func (w *response) Write(dat []byte) error {
	if !w.responded {
		if err := w.writeHeader(ResponseCodeSuccess); err != nil {
			return err
		}
	}

	acc := 0
	for acc < len(dat) {
		n, err := w.writer.Write(dat[acc:])
		if err != nil {
			return err
		}
		acc += n
	}
	return nil
}

// drain reads the rest of the request frame if not consumed by the handler.
func (w *response) drain(ctx context.Context) error {
	if reader, ok := w.req.Body.(*io.LimitedReader); ok {
		if reader.N == 0 {
			return nil
		}
		// todo: wrap body in a context reader.
		_, err := io.CopyN(io.Discard, w.req.Body, reader.N)
		if err == nil || err == io.EOF {
			return nil
		}
		return err
	}
	return io.ErrUnexpectedEOF
}

func (w *response) finish(ctx context.Context) error {
	select {
	case w.conn.writeSerializer <- w.writer.Bytes():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// maxRequestSize bounds a single RPC record's length. Reading concurrently
// means up to MaxConcurrentRequests record buffers may be live at once, so an
// unbounded length from the wire could be used to exhaust memory. The largest
// legitimate request is a WRITE carrying up to the advertised wtmax (1<<30)
// plus RPC/NFS headers; this leaves generous headroom above that.
const maxRequestSize = (1 << 30) + (1 << 16)

// maxPooledRecord caps the capacity of a record buffer returned to the pool.
// Buffers grown to serve a large WRITE (up to wtmax) are not retained, so the
// pool's idle footprint stays bounded by the common (small) request size rather
// than the largest one ever seen.
const maxPooledRecord = 1 << 20

// recordBufPool recycles the per-request record buffers read off the wire.
// ntirpc reuses a per-connection input stream (svc_vc's xdrs_in) to avoid
// allocating a buffer per request; in Go a shared buffer would race the
// concurrent workers, so each request instead borrows a buffer from this pool
// and returns it once handling completes (response.release). Pooling *[]byte
// (not []byte) avoids an allocation on every Put.
var recordBufPool = sync.Pool{New: func() any { b := []byte(nil); return &b }}

// getRecordBuf returns a buffer of length n, reusing a pooled one when large
// enough.
func getRecordBuf(n int) *[]byte {
	bp := recordBufPool.Get().(*[]byte)
	if cap(*bp) < n {
		*bp = make([]byte, n)
	} else {
		*bp = (*bp)[:n]
	}

	return bp
}

// putRecordBuf returns a record buffer to the pool, dropping oversized ones so
// idle memory is not pinned at the largest WRITE seen.
func putRecordBuf(bp *[]byte) {
	if bp == nil || cap(*bp) > maxPooledRecord {
		return
	}
	recordBufPool.Put(bp)
}

func (c *conn) readRequestHeader(ctx context.Context, reader *bufio.Reader) (w *response, err error) {
	fragment, err := xdr.ReadUint32(reader)
	if err != nil {
		if xdrErr, ok := err.(*xdr2.UnmarshalError); ok {
			if xdrErr.Err == io.EOF {
				return nil, io.EOF
			}
		}
		return nil, err
	}
	if fragment&(1<<31) == 0 {
		Log.Warnf("Warning: haven't implemented fragment reconstruction.\n")
		return nil, ErrInputInvalid
	}
	reqLen := fragment - uint32(1<<31)
	if reqLen < 40 {
		return nil, ErrInputInvalid
	}
	if reqLen > maxRequestSize {
		return nil, ErrInputInvalid
	}

	// Copy the whole record off the shared socket reader into a pooled private
	// buffer before returning. Handlers run on worker goroutines concurrently
	// with this loop reading the next record, so the body must NOT alias the
	// shared bufio.Reader — each request owns its bytes (cf. knfsd's per-thread
	// rq_arg, nfs-ganesha's per-connection xdrs_in). The buffer is returned to the
	// pool by response.release once handling completes; xdr decodes opaque fields
	// (handle/data) into fresh slices, so nothing aliases it after that. A short
	// read mid-record is a truncated/closed connection, not a clean close.
	bufp := getRecordBuf(int(reqLen))
	if _, err := io.ReadFull(reader, *bufp); err != nil {
		putRecordBuf(bufp)
		return nil, err
	}
	body := bytes.NewReader(*bufp)

	xid, err := xdr.ReadUint32(body)
	if err != nil {
		putRecordBuf(bufp)
		return nil, err
	}
	reqType, err := xdr.ReadUint32(body)
	if err != nil {
		putRecordBuf(bufp)
		return nil, err
	}
	if reqType != 0 { // 0 = request, 1 = response
		putRecordBuf(bufp)
		return nil, ErrInputInvalid
	}

	req := request{
		xid,
		rpc.Header{},
		// LimitedReader so response.drain's type assertion still applies; N is the
		// bytes left after the header, i.e. the request body the handler consumes.
		&io.LimitedReader{R: body, N: int64(body.Len())},
	}
	if err = xdr.Read(req.Body, &req.Header); err != nil {
		putRecordBuf(bufp)
		return nil, err
	}

	w = &response{
		conn:      c,
		req:       &req,
		errorFmt:  basicErrorFormatter,
		recordBuf: bufp,
		// TODO: use a pool for these.
		writer: bytes.NewBuffer([]byte{}),
	}
	return w, nil
}
