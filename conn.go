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
	defer func() {
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

	c.writeSerializer = make(chan []byte, 1)
	go c.serializeWrites(connCtx)

	bio := bufio.NewReader(c.Conn)
	for {
		w, err := c.readRequestHeader(connCtx, bio)
		if err != nil {
			if err == io.EOF {
				// Clean close.
				c.Close()
				return
			}
			return
		}
		Log.Tracef("request: %v", w.req)
		err = c.handle(connCtx, w)
		respErr := w.finish(connCtx)
		if err != nil {
			Log.Errorf("error handling req: %v", err)
			// failure to handle at a level needing to close the connection.
			c.Close()
			return
		}
		if respErr != nil {
			Log.Errorf("error sending response: %v", respErr)
			c.Close()
			return
		}
	}
}

func (c *conn) serializeWrites(ctx context.Context) {
	// todo: maybe don't need the extra buffer
	writer := bufio.NewWriter(c.Conn)
	var fragmentBuf [4]byte
	var fragmentInt uint32
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-c.writeSerializer:
			if !ok {
				return
			}
			// prepend the fragmentation header
			fragmentInt = uint32(len(msg))
			fragmentInt |= (1 << 31)
			binary.BigEndian.PutUint32(fragmentBuf[:], fragmentInt)
			n, err := writer.Write(fragmentBuf[:])
			if n < 4 || err != nil {
				return
			}
			n, err = writer.Write(msg)
			if err != nil {
				return
			}
			if n < len(msg) {
				panic("todo: ensure writes complete fully.")
			}
			if err = writer.Flush(); err != nil {
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

	r := io.LimitedReader{R: reader, N: int64(reqLen)}

	xid, err := xdr.ReadUint32(&r)
	if err != nil {
		return nil, err
	}
	reqType, err := xdr.ReadUint32(&r)
	if err != nil {
		return nil, err
	}
	if reqType != 0 { // 0 = request, 1 = response
		return nil, ErrInputInvalid
	}

	req := request{
		xid,
		rpc.Header{},
		&r,
	}
	if err = xdr.Read(&r, &req.Header); err != nil {
		return nil, err
	}

	w = &response{
		conn:     c,
		req:      &req,
		errorFmt: basicErrorFormatter,
		// TODO: use a pool for these.
		writer: bytes.NewBuffer([]byte{}),
	}
	return w, nil
}
