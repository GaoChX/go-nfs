package nfs_test

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	osfs "github.com/go-git/go-billy/v5/osfs"
	nfs "github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs/helpers"

	nfsc "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

const nfsProcWrite = 7

// buildWriteCall encodes a complete NFSv3 WRITE call record for handle at the
// given offset, writing len(data) bytes with UNSTABLE stability (How=0).
func buildWriteCall(xid uint32, handle []byte, offset uint64, data []byte) []byte {
	body := bytes.NewBuffer(nil)
	_ = xdr.Write(body, xid)
	_ = xdr.Write(body, uint32(0)) // msg type: call
	_ = xdr.Write(body, rpc.Header{
		Rpcvers: 2,
		Prog:    nfsServiceID,
		Vers:    nfsVers,
		Proc:    nfsProcWrite,
		Cred:    rpc.AuthNull,
		Verf:    rpc.AuthNull,
	})
	// WRITE3args: handle, offset, count, stable(how), data<>
	_ = xdr.Write(body, handle)
	_ = xdr.Write(body, offset)
	_ = xdr.Write(body, uint32(len(data)))
	_ = xdr.Write(body, uint32(0)) // UNSTABLE
	_ = xdr.Write(body, data)

	rec := bytes.NewBuffer(nil)
	var fragBuf [4]byte
	binary.BigEndian.PutUint32(fragBuf[:], uint32(body.Len())|(1<<31))
	rec.Write(fragBuf[:])
	rec.Write(body.Bytes())

	return rec.Bytes()
}

// drainReplies reads reply records off r until count are seen or it errors.
func drainReplies(r *bufio.Reader, count int, done chan<- int) {
	seen := 0
	for seen < count {
		frag, err := xdr.ReadUint32(r)
		if err != nil {
			break
		}
		n := frag &^ (1 << 31)
		if _, err := io.CopyN(io.Discard, r, int64(n)); err != nil {
			break
		}
		seen++
	}
	done <- seen
}

// BenchmarkConcurrentWrites drives many pipelined UNSTABLE WRITEs over a single
// connection against an osfs-backed server and captures CPU + heap profiles.
// Run with: go test -run x -bench BenchmarkConcurrentWrites -benchtime 10s
func BenchmarkConcurrentWrites(b *testing.B) {
	dir := b.TempDir()
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		b.Fatal(err)
	}
	defer listener.Close()

	bfs := osfs.New(dir)
	handler := helpers.NewNullAuthHandler(bfs)
	cacheHelper := helpers.NewCachingHandler(handler, 1024)
	srv := &nfs.Server{Handler: cacheHelper}
	go func() { _ = srv.Serve(listener) }()

	// Use the high-level client just to mount and create the target file, then
	// grab its opaque handle for raw pipelined writes.
	c, err := rpc.DialTCP(listener.Addr().Network(), listener.Addr().(*net.TCPAddr).String(), false)
	if err != nil {
		b.Fatal(err)
	}
	var mounter nfsc.Mount
	mounter.Client = c
	target, err := mounter.Mount("/", rpc.AuthNull)
	if err != nil {
		b.Fatal(err)
	}
	handle, err := target.Create("bench.dat", os.FileMode(0o644))
	if err != nil {
		b.Fatal(err)
	}
	_ = mounter.Unmount()
	c.Close()

	// Raw connection for pipelined writes.
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	payload := make([]byte, 4096)
	const window = 64 // outstanding requests in flight

	cpuF, _ := os.Create("/tmp/nfs_cpu.prof")
	defer cpuF.Close()
	_ = pprof.StartCPUProfile(cpuF)
	defer pprof.StopCPUProfile()

	var xid atomic.Uint32
	repliesDone := make(chan int, 1)
	br := bufio.NewReader(conn)
	go drainReplies(br, b.N, repliesDone)

	b.SetBytes(int64(len(payload)))
	b.ResetTimer()

	// A bounded window of writer goroutines keeps `window` requests outstanding.
	var wg sync.WaitGroup
	sem := make(chan struct{}, window)
	var sent atomic.Int64
	writeMu := sync.Mutex{} // serialize socket writes (one conn)
	for sent.Load() < int64(b.N) {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			n := sent.Add(1)
			if n > int64(b.N) {
				return
			}
			id := xid.Add(1)
			off := (uint64(id) % 4096) * uint64(len(payload))
			rec := buildWriteCall(id, handle, off, payload)
			writeMu.Lock()
			_, _ = conn.Write(rec)
			writeMu.Unlock()
		}()
	}
	wg.Wait()

	select {
	case <-repliesDone:
	case <-time.After(30 * time.Second):
		b.Fatal("timed out waiting for replies")
	}
	b.StopTimer()

	heapF, _ := os.Create("/tmp/nfs_heap.prof")
	defer heapF.Close()
	runtime.GC()
	_ = pprof.WriteHeapProfile(heapF)
}
