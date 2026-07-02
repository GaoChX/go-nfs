package helpers

import (
	"context"
	"strconv"
	"testing"

	"github.com/go-git/go-billy/v5/memfs"
)

// BenchmarkFromHandle measures the per-request cost of resolving an opaque
// handle back to its path, which every WRITE/READ/COMMIT pays. It is run with a
// progressively larger active-handle cache to expose any cost that scales with
// the number of cached handles (the cache is shared, so such cost also
// serializes concurrent requests on the cache's internal lock).
func benchFromHandleN(b *testing.B, n int) {
	fs := memfs.New()
	h := NewCachingHandler(&NullAuthHandler{}, n+16).(*CachingHandler)
	ctx := context.Background()
	var probe []byte
	for i := 0; i < n; i++ {
		hd := h.ToHandle(ctx, fs, []string{"d", strconv.Itoa(i), "f"})
		if i == n/2 {
			probe = hd
		}
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, _ = h.FromHandle(ctx, probe)
		}
	})
}

func BenchmarkFromHandle16(b *testing.B)  { benchFromHandleN(b, 16) }
func BenchmarkFromHandle1k(b *testing.B)  { benchFromHandleN(b, 1024) }
func BenchmarkFromHandle16k(b *testing.B) { benchFromHandleN(b, 16384) }
