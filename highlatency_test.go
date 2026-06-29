package nfs_test

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	osfs "github.com/go-git/go-billy/v5/osfs"
	nfs "github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs/helpers"
)

// TestHighLatencyBackendConcurrency drives UNSTABLE WRITEs through the real
// server over a backend with injected per-write latency (simulating JuiceFS) and
// checks that the server keeps many writes in flight at once. On a high-latency
// backend, throughput = concurrency / latency (Little's law), so the achieved
// in-flight count is the thing that decides whether the backend is saturated.
//
// Two regimes are compared to prove the FromHandle fix matters under latency:
//   - many files spread across connections (the nconnect write pattern)
//
// The server caps concurrency at MaxConcurrentRequests per connection, so the
// ceiling is conns * min(window, MaxConcurrentRequests).
func TestHighLatencyBackendConcurrency(t *testing.T) {
	// The assertions depend on real wall-clock concurrency. The race detector
	// serializes goroutine scheduling enough to suppress achievable in-flight
	// depth below the thresholds, so skip it there (correctness is covered by the
	// other race tests; this one measures throughput/concurrency).
	if raceEnabled {
		t.Skip("timing-sensitive; skipped under -race")
	}
	const (
		files   = 512
		conns   = 8
		window  = 16
		latency = 2 * time.Millisecond
		perConn = 2000 // writes each connection issues
	)

	dir := t.TempDir()
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	lfs := newLaggyFS(osfs.New(dir), latency)
	handler := helpers.NewNullAuthHandler(lfs)
	cacheHelper := helpers.NewCachingHandler(handler, files+16)
	srv := &nfs.Server{Handler: cacheHelper}
	go func() { _ = srv.Serve(listener) }()

	// Pre-create files and mint handles (fills the handle cache to `files`).
	handles := make([][]byte, files)
	ctx := context.Background()
	for i := 0; i < files; i++ {
		name := fileName(i)
		f, cerr := lfs.Create(name)
		if cerr != nil {
			t.Fatalf("create %d: %v", i, cerr)
		}
		_ = f.Close()
		handles[i] = cacheHelper.ToHandle(ctx, lfs, []string{name})
	}
	// Reset counters so setup Creates do not skew the in-flight measurement.
	lfs.writes.Store(0)
	lfs.maxInFlight.Store(0)
	lfs.inFlight.Store(0)

	payload := make([]byte, 4096)
	start := time.Now()
	var sent atomic.Int64
	total := int64(conns * perConn)

	var wg sync.WaitGroup
	for c := 0; c < conns; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			driveConnWrites(&benchShim{t}, listener.Addr().String(), handles, payload, window, &sent, total)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	maxIF := lfs.maxInFlight.Load()
	writes := lfs.writes.Load()
	ceiling := int64(conns * minInt(window, nfs.DefaultMaxConcurrentRequests))
	throughput := float64(writes) / elapsed.Seconds()

	t.Logf("backend latency=%v writes=%d elapsed=%v", latency, writes, elapsed)
	t.Logf("max writes in flight=%d (ceiling=%d)", maxIF, ceiling)
	t.Logf("throughput=%.0f writes/s (serial would be %.0f)", throughput, 1.0/latency.Seconds())

	// If the server serialized writes, maxInFlight would be ~1 and throughput
	// ~1/latency. We require it to reach a large fraction of the ceiling, proving
	// concurrency actually reaches the backend.
	if maxIF < ceiling/2 {
		t.Errorf("server did not parallelize writes to the latency backend: max in flight %d, want >= %d", maxIF, ceiling/2)
	}
	// Throughput must beat the serial bound by a wide margin.
	if serial := 1.0 / latency.Seconds(); throughput < serial*4 {
		t.Errorf("throughput %.0f writes/s barely above serial bound %.0f: writes are serializing", throughput, serial)
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}

	return b
}

// benchShim adapts *testing.T to the *testing.B-typed error path used by
// driveConnWrites (it only calls Errorf on dial failure).
type benchShim struct{ t *testing.T }

func (s *benchShim) Errorf(format string, args ...any) { s.t.Errorf(format, args...) }
