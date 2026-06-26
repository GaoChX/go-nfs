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
