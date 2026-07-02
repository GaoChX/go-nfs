package nfs

import (
	"strconv"
	"sync/atomic"
	"testing"

	billy "github.com/go-git/go-billy/v5"
)

// nopFile is a billy.File whose Close is a no-op, so cached handles can be
// flushed/evicted by the writeCache without a real fd.
type nopFile struct{ billy.File }

func (nopFile) Close() error { return nil }

// BenchmarkWriteCacheLookupPattern isolates the writeCache mutex cost of the
// onWrite hot path. Each iteration reproduces the handle lookups a single
// already-cached WRITE performs:
//
//	before: two get()s (pre-op wcc fstat, then cachedWrite's first attempt)
//	after:  one get() (the pre-op handle is reused as cachedWrite's hint)
//
// Both variants then do the same trivial work, so the delta is purely the
// second lock acquisition. Run highly parallel to expose contention on the
// single per-connection mutex:
//
//	go test -run x -bench BenchmarkWriteCacheLookupPattern -cpu 16
func benchWriteCacheLookups(b *testing.B, gets int) {
	c := newWriteCache(nil)
	defer c.Close()

	// Populate a working set of cached handles the workers rotate through, so
	// lookups hit (the common steady-state case) rather than miss.
	const files = 256
	keys := make([]string, files)
	for i := 0; i < files; i++ {
		k := "handle-" + strconv.Itoa(i)
		keys[i] = k
		c.put(k, &cachedHandle{file: nopFile{}})
	}

	var ctr atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := keys[int(ctr.Add(1))%files]
			var h *cachedHandle
			for g := 0; g < gets; g++ {
				h = c.get(key)
			}
			if h == nil {
				b.Fatal("unexpected cache miss")
			}
		}
	})
}

// BenchmarkWriteCacheLookupBefore models the pre-optimization path (two locked
// lookups per WRITE).
func BenchmarkWriteCacheLookupBefore(b *testing.B) { benchWriteCacheLookups(b, 2) }

// BenchmarkWriteCacheLookupAfter models the optimized path (one locked lookup
// per WRITE; the pre-op handle is reused as the cachedWrite hint).
func BenchmarkWriteCacheLookupAfter(b *testing.B) { benchWriteCacheLookups(b, 1) }
