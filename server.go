package nfs

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultMaxConcurrentRequests is the per-connection request concurrency used
// when Server.MaxConcurrentRequests is zero. Throughput on a high-latency
// backend is concurrency / latency (Little's law), so this is the per-connection
// ceiling on in-flight backend operations; a single pipelined/nconnect mount
// cannot exceed it no matter how deep the client queues.
//
// 64 balances that against memory: a connection retains up to this many request
// record buffers, each pooled at maxPooledRecord (1 MiB) — so ~64 MiB worst case
// per connection at steady state. Embedders with very many connections may want
// it lower; those serving a few high-depth mounts (e.g. an nconnect proxy in
// front of an object-store-backed FS) may want it higher.
const DefaultMaxConcurrentRequests = 64

// Server is a handle to the listening NFS server.
type Server struct {
	Handler
	ID [8]byte
	context.Context

	OnConnect    func(ctx context.Context, conn net.Conn) (context.Context, net.Conn)
	OnDisconnect func(ctx context.Context, conn net.Conn)

	// MaxConcurrentRequests bounds how many requests a single connection
	// processes in parallel. A connection reads each RPC record off the wire
	// serially (one reader per socket) but then dispatches it to a worker pool,
	// so a run of pipelined requests — typical of an nconnect mount under load —
	// no longer waits for each predecessor's disk IO to finish before the next is
	// picked up. Replies are matched by XID and may return out of order, so the
	// only ceiling on a single connection's IOPS becomes this many in-flight disk
	// operations rather than one. Larger values raise throughput at the cost of
	// holding that many request bodies (up to wsize each) in memory concurrently.
	// Zero selects DefaultMaxConcurrentRequests.
	MaxConcurrentRequests int

	// conns tracks live connections so a handle-mutating op (SETATTR/REMOVE/
	// RENAME) on one connection can invalidate cached fds for that handle on
	// every other connection too. Per-connection caches keep fd lifecycle
	// simple (drained on disconnect), so cross-connection invalidation is a
	// broadcast rather than a shared cache.
	connsMu sync.Mutex
	conns   map[*conn]struct{}

	// commits records mount-wide durability state for unstable writes, keyed by
	// NFS file handle, so a COMMIT on any nconnect connection observes flush
	// failures from handle evictions/drains that happened on another connection
	// (see commitTracker). Lazily initialized via commitsTracker.
	commitsOnce sync.Once
	commits     *commitTracker

	// writeVerifier is the NFSv3 write verifier (writeverf3) stamped into every
	// WRITE and COMMIT reply. RFC 1813 requires it to be constant within a single
	// server instance and to change when the server can no longer guarantee the
	// durability of previously-acknowledged UNSTABLE writes, so a client comparing
	// the verifier from its WRITE replies against the one in a later COMMIT reply
	// learns it must replay. It is seeded on first use and rotated (via
	// rotateWriteVerifier) whenever a handle eviction/drain loses still-dirty data
	// — modeled on knfsd's commit_reset_write_verifier. An atomic pointer because
	// any connection's goroutine may read it (WRITE/COMMIT) while another rotates
	// it on a flush failure; a nil pointer is the unseeded state, so a zero-value
	// Server (tests) lazily seeds a stable verifier on first read.
	writeVerifier atomic.Pointer[[8]byte]
}

// randomVerifier returns 8 random bytes for use as a server/write verifier. A
// read error leaves the buffer zeroed, which callers treat as "seed again next
// time" rather than fatal.
func randomVerifier() [8]byte {
	var v [8]byte
	_, _ = rand.Reader.Read(v[:])

	return v
}

// currentWriteVerifier returns the server's current 8-byte write verifier,
// seeding it on first use so a freshly constructed Server reports a stable
// verifier until a durability loss rotates it. The initial value is the
// configured Server.ID when an embedding application set one (preserving the
// historical behavior where WRITE/COMMIT replies returned Server.ID), and a
// random value otherwise — including the zero-value Servers used in tests.
func (s *Server) currentWriteVerifier() [8]byte {
	if v := s.writeVerifier.Load(); v != nil {
		return *v
	}
	seed := s.ID
	if seed == ([8]byte{}) {
		seed = randomVerifier()
	}
	if s.writeVerifier.CompareAndSwap(nil, &seed) {
		return seed
	}

	return *s.writeVerifier.Load()
}

// rotateWriteVerifier changes the write verifier so every client with
// outstanding UNSTABLE writes is told to replay them. Called when a flush of
// acknowledged-but-uncommitted data fails (ENOSPC/EIO) and the buffered data may
// be lost: the per-handle commit error is consumed by exactly one file's COMMIT,
// but a storage failure can drop buffered data for other files too, so rotating
// the server-wide verifier is the protocol-correct safety net (RFC 1813; knfsd
// commit_reset_write_verifier).
func (s *Server) rotateWriteVerifier() {
	next := randomVerifier()
	s.writeVerifier.Store(&next)
}

// commitTracker returns the server's mount-wide commit tracker, creating it on
// first use. The tracker's loss callback rotates the write verifier so a flush
// failure that loses acknowledged unstable data signals every client to replay.
func (s *Server) commitsTracker() *commitTracker {
	s.commitsOnce.Do(func() {
		s.commits = newCommitTracker()
		s.commits.onLoss = s.rotateWriteVerifier
	})

	return s.commits
}

// registerConn adds c to the live-connection set.
func (s *Server) registerConn(c *conn) {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()

	if s.conns == nil {
		s.conns = make(map[*conn]struct{})
	}
	s.conns[c] = struct{}{}
}

// unregisterConn removes c from the live-connection set.
func (s *Server) unregisterConn(c *conn) {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()

	delete(s.conns, c)
}

// bindHandleEviction couples the handler's handle cache to the per-connection fd
// caches: when the handler evicts a handle, any read/write fd still cached under
// it on any connection is closed. Without this the fd outlives the handle (until
// idle sweep/COMMIT), and a RENAME/REMOVE can no longer resolve the path to that
// handle to drop the fd, so a Windows-like backend's rename/remove of the still-
// open file fails. Handlers without a bounded cache (no HandleEvictionReceiver)
// need no coupling. Called once at Serve start.
func (s *Server) bindHandleEviction() {
	if r, ok := s.Handler.(HandleEvictionReceiver); ok {
		r.SetHandleEvictionCallback(s.dropHandleAll)
	}
}

// dropHandleAll flushes/closes any cached read/write handles for the given NFS
// file handle across every live connection. Called after a handle-mutating op
// so a cached fd opened under the old attributes (permissions/size) on another
// connection cannot be reused — the next READ/WRITE there reopens and is
// re-checked by the backing filesystem.
func (s *Server) dropHandleAll(handle []byte) {
	s.connsMu.Lock()
	conns := make([]*conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.connsMu.Unlock()

	for _, c := range conns {
		if err := c.dropHandle(handle); err != nil {
			Log.Errorf("error dropping cached handle across connections: %v", err)
		}
	}
}

// flushHandleAll flushes (without closing) any cached write handle for the
// given NFS file handle across every live connection, returning whether any
// connection held a cached handle. With nconnect the unstable writes for a file
// may have been cached on a different connection than the one receiving COMMIT,
// so flushing only the local cache could acknowledge data that is still buffered
// in a peer connection's fd on backends whose Sync is not inode-wide (or that
// buffer inside the billy.File). Broadcasting flushes them all. Returns the
// first sync error encountered (nil if none).
func (s *Server) flushHandleAll(handle []byte) (bool, error) {
	s.connsMu.Lock()
	conns := make([]*conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.connsMu.Unlock()

	var (
		found    bool
		firstErr error
	)
	for _, c := range conns {
		ok, err := c.flushHandle(handle)
		found = found || ok
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return found, firstErr
}

// flushDirtyHandleAll flushes any cached write handle holding unflushed unstable
// writes for the given NFS handle across every live connection, so a subsequent
// READ (served from a separate read-only fd) observes them on backends that
// buffer writes until Sync/Close. With nconnect the unstable writes for a file
// may have been cached on a different connection than the one receiving the
// READ, so this broadcasts like COMMIT. It is a no-op (just a cache lookup) when
// no connection holds a dirty handle, keeping read-only workloads cheap. Returns
// the first flush error encountered (nil if none).
func (s *Server) flushDirtyHandleAll(handle []byte) error {
	s.connsMu.Lock()
	conns := make([]*conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.connsMu.Unlock()

	var firstErr error
	for _, c := range conns {
		if _, err := c.flushDirtyHandle(handle); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return firstErr
	}

	// The connection that recorded an in-flight or failed dirty close may have
	// already drained/unregistered, so none of the per-connection flushDirty calls
	// above reached the mount-wide tracker for this handle. Await it directly
	// (non-consuming) so a READ still observes a recorded ENOSPC/EIO that COMMIT
	// would catch, instead of serving a fresh read-only fd's stale bytes. await,
	// not wait: COMMIT remains the checkpoint that consumes the error.
	return s.commitsTracker().await(string(handle))
}

// dropAndRetry runs a handle-mutating filesystem op (REMOVE/RENAME) that can be
// rejected by Windows-like backends while a file is still open. A concurrent
// READ/WRITE may cache a fresh fd for an affected path in the window between the
// pre-op drop and the op itself; if the op fails, this re-resolves the handles
// (so a path that was uncached at the first snapshot but has since been looked
// up and opened is included) and drops them across all connections before
// retrying once. os.ErrNotExist is deterministic, so it is not retried. Returns
// the final op error (nil on success).
func (s *Server) dropAndRetry(op func() error, resolveHandles func() [][]byte) error {
	err := op()
	if err == nil || os.IsNotExist(err) {
		return err
	}
	// The op may have failed because a racing READ/WRITE re-opened a file in the
	// mutation window. Re-resolve (not just re-drop the stale snapshot) so a
	// newly-cached handle for a previously-uncached path is closed too, then retry.
	for _, h := range resolveHandles() {
		s.dropHandleAll(h)
	}

	return op()
}

// RegisterMessageHandler registers a handler for a specific
// XDR procedure.
func RegisterMessageHandler(protocol uint32, proc uint32, handler HandleFunc) error {
	if registeredHandlers == nil {
		registeredHandlers = make(map[registeredHandlerID]HandleFunc)
	}
	for k := range registeredHandlers {
		if k.protocol == protocol && k.proc == proc {
			return errors.New("already registered")
		}
	}
	id := registeredHandlerID{protocol, proc}
	registeredHandlers[id] = handler
	return nil
}

// HandleFunc represents a handler for a specific protocol message.
type HandleFunc func(ctx context.Context, w *response, userHandler Handler) error

// TODO: store directly as a uint64 for more efficient lookups
type registeredHandlerID struct {
	protocol uint32
	proc     uint32
}

var registeredHandlers map[registeredHandlerID]HandleFunc

// Serve listens on the provided listener port for incoming client requests.
func (s *Server) Serve(l net.Listener) error {
	defer l.Close()
	baseCtx := context.Background()
	if s.Context != nil {
		baseCtx = s.Context
	}
	if bytes.Equal(s.ID[:], []byte{0, 0, 0, 0, 0, 0, 0, 0}) {
		if _, err := rand.Reader.Read(s.ID[:]); err != nil {
			return err
		}
	}

	s.bindHandleEviction()

	var tempDelay time.Duration

	for {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				time.Sleep(tempDelay)
				continue
			}
			return err
		}
		tempDelay = 0

		c := s.newConn(conn)
		go c.serve(baseCtx)
	}
}

func (s *Server) newConn(nc net.Conn) *conn {
	c := &conn{
		Server: s,
		Conn:   nc,
	}
	return c
}

// TODO: keep an immutable map for each server instance to have less
// chance of races.
func (s *Server) handlerFor(prog uint32, proc uint32) HandleFunc {
	for k, v := range registeredHandlers {
		if k.protocol == prog && k.proc == proc {
			return v
		}
	}
	return nil
}

// Serve is a singleton listener paralleling http.Serve
func Serve(l net.Listener, handler Handler) error {
	srv := &Server{Handler: handler}
	return srv.Serve(l)
}
