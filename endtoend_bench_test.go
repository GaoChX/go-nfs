package nfs_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	billy "github.com/go-git/go-billy/v5"
	osfs "github.com/go-git/go-billy/v5/osfs"
	nfs "github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs/helpers"
)

// readFragment reads one RPC record fragment header off r and discards its
// body, returning the body length. It returns an error on a closed/short
// connection so the reply-draining loop terminates.
func readFragment(r *bufio.Reader) (uint32, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, err
	}
	n := binary.BigEndian.Uint32(hdr[:]) &^ (1 << 31)
	if _, err := io.CopyN(io.Discard, r, int64(n)); err != nil {
		return 0, err
	}

	return n, nil
}

// endToEndConfig parameterizes a realistic fio-like write workload against the
// server: many distinct files (which populate the handle cache, exposing any
// per-request cost that scales with cache size), spread over several
// connections (nconnect), each connection keeping a window of UNSTABLE WRITEs
// in flight without waiting for replies.
type endToEndConfig struct {
	files       int // distinct target files (handle-cache occupancy)
	conns       int // simultaneous connections (nconnect)
	window      int // outstanding requests per connection
	writesPerOp int // WRITE RPCs issued per benchmark iteration unit
	payload     int // bytes per WRITE
}

// benchEndToEndWrites mounts an osfs-backed server, pre-creates cfg.files target
// files to fill the handle cache, then drives pipelined UNSTABLE WRITEs over
// cfg.conns raw connections and reports aggregate throughput.
func benchEndToEndWrites(b *testing.B, cfg endToEndConfig) {
	dir := b.TempDir()
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		b.Fatal(err)
	}
	defer listener.Close()

	bfs := osfs.New(dir)
	handler := helpers.NewNullAuthHandler(bfs)
	// Cache large enough to hold every file's handle, so FromHandle operates at
	// full occupancy for the whole run (the regime where an O(N) scan hurts).
	cacheHelper := helpers.NewCachingHandler(handler, cfg.files+16)
	srv := &nfs.Server{Handler: cacheHelper}
	go func() { _ = srv.Serve(listener) }()

	handles := createTargetFiles(b, bfs, cacheHelper, cfg.files)

	payload := make([]byte, cfg.payload)
	b.SetBytes(int64(cfg.payload))
	b.ResetTimer()

	var sent atomic.Int64
	total := int64(b.N)
	var wg sync.WaitGroup
	for ci := 0; ci < cfg.conns; ci++ {
		wg.Add(1)
		go func(connIdx int) {
			defer wg.Done()
			driveConnWrites(b, listener.Addr().String(), handles, payload, cfg.window, &sent, total)
		}(ci)
	}
	wg.Wait()
	b.StopTimer()
}

// createTargetFiles creates n files directly on the backing filesystem and
// mints their opaque handles through the caching handler, populating the handle
// cache exactly as a run of client LOOKUPs would. Creating on the billy FS
// directly (rather than via the high-level client) keeps setup deterministic and
// guarantees the files exist for the raw WRITE path.
func createTargetFiles(b *testing.B, bfs billy.Filesystem, h nfs.Handler, n int) [][]byte {
	b.Helper()
	ctx := context.Background()
	handles := make([][]byte, n)
	for i := 0; i < n; i++ {
		name := fileName(i)
		f, err := bfs.Create(name)
		if err != nil {
			b.Fatalf("create file %d: %v", i, err)
		}
		_ = f.Close()
		handles[i] = h.ToHandle(ctx, bfs, []string{name})
	}

	return handles
}

func fileName(i int) string {
	return "f" + itoa(i) + ".dat"
}

// itoa is a tiny non-allocating-ish int formatter to avoid pulling strconv into
// the hot setup path naming.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}

	return string(buf[pos:])
}

// driveConnWrites opens one raw connection and keeps `window` UNSTABLE WRITEs in
// flight, round-robining across all file handles, until the shared counter
// reaches total. A reader goroutine drains replies so the socket does not block.
// errorfer is the minimal failure-reporting surface driveConnWrites needs, so
// it can be driven from either a *testing.B (benchmarks) or a *testing.T shim
// (the high-latency concurrency test).
type errorfer interface {
	Errorf(format string, args ...any)
}

func driveConnWrites(b errorfer, addr string, handles [][]byte, payload []byte, window int, sent *atomic.Int64, total int64) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		b.Errorf("dial: %v", err)
		return
	}
	defer conn.Close()

	replied := make(chan struct{}, 1)
	go func() {
		br := bufio.NewReader(conn)
		drained := 0
		for {
			frag, err := readFragment(br)
			if err != nil {
				break
			}
			_ = frag
			drained++
		}
		_ = drained
		select {
		case replied <- struct{}{}:
		default:
		}
	}()

	sem := make(chan struct{}, window)
	var wg sync.WaitGroup
	var xid atomic.Uint32
	var writeMu sync.Mutex
	for {
		n := sent.Add(1)
		if n > total {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(seq int64) {
			defer wg.Done()
			defer func() { <-sem }()
			id := xid.Add(1)
			h := handles[seq%int64(len(handles))]
			off := (uint64(seq) % 1024) * uint64(len(payload))
			rec := buildWriteCall(id, h, off, payload)
			writeMu.Lock()
			_, _ = conn.Write(rec)
			writeMu.Unlock()
		}(n)
	}
	wg.Wait()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
}

// BenchmarkEndToEndWrites_SingleFile is the degenerate case: all writes target
// one file, so the handle cache holds one entry. FromHandle cost is invisible
// here; this is the syscall/per-file-lock ceiling.
func BenchmarkEndToEndWrites_SingleFile(b *testing.B) {
	benchEndToEndWrites(b, endToEndConfig{files: 1, conns: 4, window: 32, payload: 4096})
}

// BenchmarkEndToEndWrites_ManyFiles fills the handle cache and spreads writes
// across files and connections, the regime where the old O(N) FromHandle scan
// throttled aggregate throughput.
func BenchmarkEndToEndWrites_ManyFiles(b *testing.B) {
	benchEndToEndWrites(b, endToEndConfig{files: 4096, conns: 8, window: 32, payload: 4096})
}
