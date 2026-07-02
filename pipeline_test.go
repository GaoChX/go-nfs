package nfs_test

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	nfs "github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs/helpers"
	"github.com/willscott/go-nfs/helpers/memfs"

	"github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

const (
	nfsServiceID   = 100003
	nfsVers        = 3
	nfsProcNull    = 0
	rpcMsgCallType = 0
)

// buildNullCall encodes a complete NFSv3 NULL RPC call record (fragment header
// + body) for the given xid. NULL takes no arguments and returns no data, so it
// exercises the read/dispatch/serialize path without touching the filesystem.
func buildNullCall(t *testing.T, xid uint32) []byte {
	t.Helper()
	body := bytes.NewBuffer(nil)
	if err := xdr.Write(body, xid); err != nil {
		t.Fatalf("write xid: %v", err)
	}
	if err := xdr.Write(body, uint32(rpcMsgCallType)); err != nil {
		t.Fatalf("write msgtype: %v", err)
	}
	hdr := rpc.Header{
		Rpcvers: 2,
		Prog:    nfsServiceID,
		Vers:    nfsVers,
		Proc:    nfsProcNull,
		Cred:    rpc.AuthNull,
		Verf:    rpc.AuthNull,
	}
	if err := xdr.Write(body, hdr); err != nil {
		t.Fatalf("write header: %v", err)
	}

	rec := bytes.NewBuffer(nil)
	frag := uint32(body.Len()) | (1 << 31)
	var fragBuf [4]byte
	binary.BigEndian.PutUint32(fragBuf[:], frag)
	rec.Write(fragBuf[:])
	rec.Write(body.Bytes())

	return rec.Bytes()
}

// readReplyXID reads one RPC reply record off r and returns its xid, validating
// the fragment header and that it is an accepted reply.
func readReplyXID(t *testing.T, r *bufio.Reader) uint32 {
	t.Helper()
	frag, err := xdr.ReadUint32(r)
	if err != nil {
		t.Fatalf("read fragment: %v", err)
	}
	if frag&(1<<31) == 0 {
		t.Fatal("multi-fragment reply not expected")
	}
	n := frag &^ (1 << 31)
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("read reply body: %v", err)
	}
	br := bytes.NewReader(buf)
	xid, err := xdr.ReadUint32(br)
	if err != nil {
		t.Fatalf("read reply xid: %v", err)
	}
	replyType, err := xdr.ReadUint32(br)
	if err != nil {
		t.Fatalf("read reply type: %v", err)
	}
	if replyType != 1 { // 1 = reply
		t.Fatalf("expected reply type 1, got %d", replyType)
	}

	return xid
}

// TestPipelinedRequestsConcurrent fires many RPC records back-to-back on a
// single connection without waiting for replies, then verifies every xid is
// answered. It proves the server reads each record into a private buffer (so a
// concurrent handler does not race the shared socket reader) and that replies
// returning out of order are all delivered.
func TestPipelinedRequestsConcurrent(t *testing.T) {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	mem := memfs.New()
	r, _ := mem.Create("/test")
	r.Close()
	handler := helpers.NewNullAuthHandler(mem)
	cacheHelper := helpers.NewCachingHandler(handler, 1024)
	srv := &nfs.Server{Handler: cacheHelper}
	go func() { _ = srv.Serve(listener) }()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const count = 200
	// Send every request first, without reading any reply, so the server must be
	// able to read past an unanswered request — impossible if handling blocked the
	// reader.
	for xid := uint32(0); xid < count; xid++ {
		if _, err := conn.Write(buildNullCall(t, xid)); err != nil {
			t.Fatalf("write call %d: %v", xid, err)
		}
	}

	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(conn)
	seen := make(map[uint32]bool, count)
	for i := 0; i < count; i++ {
		xid := readReplyXID(t, br)
		if xid >= count {
			t.Fatalf("reply for unknown xid %d", xid)
		}
		if seen[xid] {
			t.Fatalf("duplicate reply for xid %d", xid)
		}
		seen[xid] = true
	}
	if len(seen) != count {
		t.Fatalf("expected %d distinct replies, got %d", count, len(seen))
	}
}

// TestPipelinedRequestsSurviveHalfClose fires many requests back-to-back, then
// half-closes the client's write side (CloseWrite → the server's read loop sees
// io.EOF) while replies may still be in flight, and verifies every reply is
// still delivered. A client that pipelines then shuts down its write side is
// still waiting to read the replies; the server must not cancel in-flight
// handlers or drop their queued replies on a clean EOF.
func TestPipelinedRequestsSurviveHalfClose(t *testing.T) {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	mem := memfs.New()
	handler := helpers.NewNullAuthHandler(mem)
	cacheHelper := helpers.NewCachingHandler(handler, 1024)
	srv := &nfs.Server{Handler: cacheHelper}
	go func() { _ = srv.Serve(listener) }()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const count = 200
	for xid := uint32(0); xid < count; xid++ {
		if _, err := conn.Write(buildNullCall(t, xid)); err != nil {
			t.Fatalf("write call %d: %v", xid, err)
		}
	}
	// Half-close the write side: the server's read loop hits io.EOF at the next
	// record boundary while handlers/replies for the 200 requests may still be in
	// flight. The read side stays open so we can still receive every reply.
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(conn)
	seen := make(map[uint32]bool, count)
	for i := 0; i < count; i++ {
		xid := readReplyXID(t, br)
		if xid >= count {
			t.Fatalf("reply for unknown xid %d", xid)
		}
		if seen[xid] {
			t.Fatalf("duplicate reply for xid %d", xid)
		}
		seen[xid] = true
	}
	if len(seen) != count {
		t.Fatalf("expected %d distinct replies after half-close, got %d", count, len(seen))
	}

	// After flushing every reply the server must close its write side, so the
	// half-closed client reads EOF instead of hanging (and the server fd is not
	// leaked in CLOSE_WAIT). The next read must return io.EOF.
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := br.ReadByte(); err != io.EOF {
		t.Fatalf("expected io.EOF after all replies (server must close), got %v", err)
	}
}

// TestMalformedRequestClosesConnectionWithoutHang verifies the abnormal-teardown
// path closes the socket instead of hanging. The client sends a malformed
// (oversized) fragment header — which makes the read loop exit on a non-EOF
// error — and then never reads any replies. If the serializer were blocked in a
// socket write and the teardown only cancelled the context without closing the
// socket, serve would hang forever. The server must close the connection, which
// the client observes as its own read returning (EOF or error) promptly.
func TestMalformedRequestClosesConnectionWithoutHang(t *testing.T) {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	mem := memfs.New()
	handler := helpers.NewNullAuthHandler(mem)
	cacheHelper := helpers.NewCachingHandler(handler, 1024)
	srv := &nfs.Server{Handler: cacheHelper}
	go func() { _ = srv.Serve(listener) }()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send a few valid requests so handlers run and queue replies we never read.
	for xid := uint32(0); xid < 8; xid++ {
		if _, err := conn.Write(buildNullCall(t, xid)); err != nil {
			t.Fatalf("write call %d: %v", xid, err)
		}
	}

	// Now send a malformed last-fragment header claiming an oversized record
	// (> maxRequestSize). readRequestHeader rejects it with a non-EOF error,
	// driving the abnormal-teardown path.
	var frag [4]byte
	binary.BigEndian.PutUint32(frag[:], uint32(1<<31)|uint32(0x7fffffff))
	if _, err := conn.Write(frag[:]); err != nil {
		t.Fatalf("write malformed frag: %v", err)
	}

	// The server must close the connection promptly. A read with a deadline must
	// return (EOF or a connection error) rather than block forever — proving the
	// teardown closed the socket instead of hanging on the stuck serializer.
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(conn)
	for {
		if _, err := br.ReadByte(); err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				t.Fatal("server did not close the connection after a malformed request; teardown hung")
			}
			// EOF or connection reset: the server closed as expected.
			break
		}
	}
}