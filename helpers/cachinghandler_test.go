package helpers

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/go-git/go-billy/v5"
	"github.com/google/uuid"
	"github.com/willscott/go-nfs/helpers/memfs"
)

// TestCachingHandlerConcurrentToHandle tests that concurrent calls to ToHandle
// are thread-safe. Run with -race flag to detect data races:
//
//	go test -race -run TestCachingHandlerConcurrentToHandle ./helpers/
func TestCachingHandlerConcurrentToHandle(t *testing.T) {
	mem := memfs.New()
	handler := NewNullAuthHandler(mem)
	cacheHandler := NewCachingHandler(handler, 1024).(*CachingHandler)

	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				// Each goroutine creates handles for different paths
				// but also accesses some shared paths to maximize contention
				path := []string{fmt.Sprintf("file-%d-%d.txt", id, j)}
				_ = cacheHandler.ToHandle(t.Context(), mem, path)

				// Also access a shared path to increase contention
				sharedPath := []string{fmt.Sprintf("shared-%d.txt", j%10)}
				_ = cacheHandler.ToHandle(t.Context(), mem, sharedPath)
			}
		}(i)
	}

	wg.Wait()
}

// TestCachingHandlerConcurrentToHandleAndFromHandle tests concurrent access
// to both ToHandle and FromHandle methods.
func TestCachingHandlerConcurrentToHandleAndFromHandle(t *testing.T) {
	mem := memfs.New()
	handler := NewNullAuthHandler(mem)
	cacheHandler := NewCachingHandler(handler, 1024).(*CachingHandler)

	const numGoroutines = 10
	const numOperations = 100

	// Pre-create some handles
	handles := make([][]byte, 20)
	for i := 0; i < 20; i++ {
		path := []string{fmt.Sprintf("precreated-%d.txt", i)}
		handles[i] = cacheHandler.ToHandle(t.Context(), mem, path)
	}

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Writers - create new handles
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				path := []string{fmt.Sprintf("new-file-%d-%d.txt", id, j)}
				_ = cacheHandler.ToHandle(t.Context(), mem, path)
			}
		}(i)
	}

	// Readers - read existing handles
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				handle := handles[j%len(handles)]
				_, _, _ = cacheHandler.FromHandle(t.Context(), handle)
			}
		}(i)
	}

	wg.Wait()
}

// TestCachingHandlerConcurrentInvalidateHandle tests concurrent access
// when handles are being invalidated.
func TestCachingHandlerConcurrentInvalidateHandle(t *testing.T) {
	mem := memfs.New()
	handler := NewNullAuthHandler(mem)
	cacheHandler := NewCachingHandler(handler, 1024).(*CachingHandler)

	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Create and invalidate handles concurrently
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				path := []string{fmt.Sprintf("invalidate-%d-%d.txt", id, j)}
				handle := cacheHandler.ToHandle(t.Context(), mem, path)
				// Immediately invalidate some handles
				if j%3 == 0 {
					_ = cacheHandler.InvalidateHandle(t.Context(), mem, handle)
				}
			}
		}(i)
	}

	// Concurrent ToHandle calls on shared paths
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				sharedPath := []string{fmt.Sprintf("shared-invalidate-%d.txt", j%20)}
				_ = cacheHandler.ToHandle(t.Context(), mem, sharedPath)
			}
		}(i)
	}

	wg.Wait()
}

// TestCachingHandlerReflectDeepEqualRace tests for the race condition in
// searchReverseCache where reflect.DeepEqual reads filesystem internal state
// while another goroutine modifies it through file operations.
//
// The race occurs at line 108: reflect.DeepEqual(candidate.f, f)
// reflect.DeepEqual traverses all internal fields of the filesystem objects,
// including mutable maps that can be modified concurrently.
//
// Note: This test uses separate filesystems per writer goroutine to avoid
// triggering races in memfs itself (which is not thread-safe).
//
// Run with: go test -race -run TestCachingHandlerReflectDeepEqualRace ./helpers/
func TestCachingHandlerReflectDeepEqualRace(t *testing.T) {
	// Create filesystem instances - one shared for reads, others for writes
	const numWriterFS = 10
	readerFS := memfs.New()
	writerFilesystems := make([]billy.Filesystem, numWriterFS)
	for i := range writerFilesystems {
		writerFilesystems[i] = memfs.New()
	}

	handler := NewNullAuthHandler(readerFS)
	cacheHandler := NewCachingHandler(handler, 1024).(*CachingHandler)

	const numGoroutines = 10
	const numOperations = 100

	// Pre-populate cache with handles from all filesystems using same paths
	// This ensures searchReverseCache will compare different FS instances
	for j := 0; j < 10; j++ {
		path := []string{fmt.Sprintf("shared-%d.txt", j)}
		_ = cacheHandler.ToHandle(t.Context(), readerFS, path)
		for _, fs := range writerFilesystems {
			_ = cacheHandler.ToHandle(t.Context(), fs, path)
		}
	}

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Group 1: ToHandle calls triggering filesystem comparisons in searchReverseCache
	// With reflect.DeepEqual, this would race with writers modifying FS internals
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				// Alternate between reader and writer filesystems
				var fs billy.Filesystem
				if j%2 == 0 {
					fs = readerFS
				} else {
					fs = writerFilesystems[j%numWriterFS]
				}
				path := []string{fmt.Sprintf("shared-%d.txt", j%10)}
				_ = cacheHandler.ToHandle(t.Context(), fs, path)
			}
		}(i)
	}

	// Group 2: File operations on dedicated filesystems (one per goroutine)
	// Each goroutine has its own filesystem to avoid memfs internal races
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			fs := writerFilesystems[id%numWriterFS]
			for j := 0; j < numOperations; j++ {
				filename := fmt.Sprintf("/file-%d-%d.txt", id, j)
				f, err := fs.Create(filename)
				if err == nil {
					_, _ = f.Write([]byte("data"))
					_ = f.Close()
				}
			}
		}(i)
	}

	wg.Wait()
}

// TestCachingHandlerSliceReferenceRace tests for the race condition where
// getReverseHandles returns a slice reference that can be modified while
// being iterated in searchReverseCache.
//
// The race occurs because:
// 1. getReverseHandles returns c.reverseHandles[path] - a reference to the slice
// 2. After releasing RLock, searchReverseCache iterates over this slice
// 3. Concurrent appendReverseHandle/evictReverseCache modify the same slice
//
// Run with: go test -race -run TestCachingHandlerSliceReferenceRace ./helpers/
func TestCachingHandlerSliceReferenceRace(t *testing.T) {
	mem := memfs.New()
	handler := NewNullAuthHandler(mem)
	cacheHandler := NewCachingHandler(handler, 1024).(*CachingHandler)

	const numGoroutines = 20
	const numOperations = 500

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// All goroutines use the same small set of paths to maximize contention
	// on the same reverseHandles slice entries
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				// Use only 5 unique paths to maximize slice contention
				path := []string{fmt.Sprintf("race-test-%d.txt", j%5)}
				handle := cacheHandler.ToHandle(t.Context(), mem, path)

				// Occasionally invalidate to trigger evictReverseCache
				// while other goroutines are in searchReverseCache
				if j%7 == 0 {
					_ = cacheHandler.InvalidateHandle(t.Context(), mem, handle)
				}
			}
		}(i)
	}

	wg.Wait()
}

// When the handle cache evicts an entry under LRU pressure, the registered
// eviction callback must fire with that entry's opaque handle, so the server can
// close any per-connection fd still cached under it. Without this coupling a
// RENAME/REMOVE could no longer resolve the path to the (now-gone) handle and the
// fd would leak until idle sweep, failing rename/remove on Windows-like backends.
func TestHandleEvictionCallbackFiresOnLRUEviction(t *testing.T) {
	mem := memfs.New()
	handler := NewNullAuthHandler(mem)
	// Small cache so a few handles force an eviction.
	cacheHandler := NewCachingHandler(handler, 2).(*CachingHandler)

	var (
		mu      sync.Mutex
		evicted [][]byte
	)
	cacheHandler.SetHandleEvictionCallback(func(h []byte) {
		mu.Lock()
		evicted = append(evicted, append([]byte(nil), h...))
		mu.Unlock()
	})

	// The first handle is the LRU victim once the cache (size 2) overflows.
	first := cacheHandler.ToHandle(t.Context(), mem, []string{"a.txt"})
	cacheHandler.ToHandle(t.Context(), mem, []string{"b.txt"})
	cacheHandler.ToHandle(t.Context(), mem, []string{"c.txt"}) // evicts a.txt

	mu.Lock()
	defer mu.Unlock()
	if len(evicted) != 1 {
		t.Fatalf("expected exactly one eviction callback, got %d", len(evicted))
	}
	if !bytes.Equal(evicted[0], first) {
		t.Fatalf("eviction callback fired with %x, want evicted handle %x", evicted[0], first)
	}
}

// With no eviction callback registered, LRU eviction must still work (the
// callback is optional; handlers without a bounded fd coupling need none).
func TestHandleEvictionNoCallbackSafe(t *testing.T) {
	mem := memfs.New()
	handler := NewNullAuthHandler(mem)
	cacheHandler := NewCachingHandler(handler, 2).(*CachingHandler)

	// No SetHandleEvictionCallback: overflowing the cache must not panic.
	cacheHandler.ToHandle(t.Context(), mem, []string{"a.txt"})
	cacheHandler.ToHandle(t.Context(), mem, []string{"b.txt"})
	cacheHandler.ToHandle(t.Context(), mem, []string{"c.txt"})
}

// Under concurrent ToHandle calls that overflow a small cache, the eviction
// callback must fire for exactly the handles the cache actually evicted — never a
// handle still live in the cache. The previous GetOldest-then-Add approach could
// report the wrong victim under this race (closing the wrong fd); routing through
// the LRU's own callback fixes it. Run with -race.
func TestHandleEvictionCallbackMatchesCacheUnderConcurrency(t *testing.T) {
	mem := memfs.New()
	handler := NewNullAuthHandler(mem)
	const cacheSize = 8
	cacheHandler := NewCachingHandler(handler, cacheSize).(*CachingHandler)

	var (
		mu      sync.Mutex
		evicted = map[uuid.UUID]struct{}{}
	)
	cacheHandler.SetHandleEvictionCallback(func(h []byte) {
		id, err := uuid.FromBytes(h)
		if err != nil {
			t.Errorf("callback got bad handle bytes: %v", err)
			return
		}
		mu.Lock()
		evicted[id] = struct{}{}
		mu.Unlock()
	})

	const numGoroutines = 8
	const numOps = 200
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				path := []string{fmt.Sprintf("g%d-f%d.txt", g, j)}
				cacheHandler.ToHandle(t.Context(), mem, path)
			}
		}(g)
	}
	wg.Wait()

	// Every handle reported as evicted must NOT still be live in the cache: a
	// wrong-victim notification would name a handle the cache still holds.
	mu.Lock()
	defer mu.Unlock()
	for id := range evicted {
		if _, ok := cacheHandler.activeHandles.Peek(id); ok {
			t.Fatalf("handle %x reported evicted but still present in cache", id)
		}
	}
}

// TestFromHandleRepinsAncestors verifies the semantic preserved from upstream's
// "re-pin to root on accesses": resolving a child handle bumps the LRU recency
// of its ancestor directory handles, so an ancestor is not evicted from under a
// child that is still in use. Without the re-pin, accessing only the child
// leaves the parent as the oldest entry, so the next insertion evicts it and the
// client is later handed a stale parent handle.
func TestFromHandleRepinsAncestors(t *testing.T) {
	mem := memfs.New()
	handler := NewNullAuthHandler(mem)
	// Cache of exactly 2 so that, with the parent and child both cached, the
	// next insertion forces one eviction.
	cacheHandler := NewCachingHandler(handler, 2).(*CachingHandler)
	ctx := t.Context()

	parent := cacheHandler.ToHandle(ctx, mem, []string{"dir"})
	child := cacheHandler.ToHandle(ctx, mem, []string{"dir", "file.txt"})

	// Touch the child. The re-pin must bump "dir" so it is no longer the LRU
	// victim even though it was inserted before the child.
	if _, _, err := cacheHandler.FromHandle(ctx, child); err != nil {
		t.Fatalf("FromHandle(child) failed: %v", err)
	}

	// Insert a third, unrelated handle: with the cache full this evicts the LRU
	// entry. The re-pin made the parent newer than the child, so the parent must
	// survive (the child is the victim instead).
	cacheHandler.ToHandle(ctx, mem, []string{"other.txt"})

	if _, _, err := cacheHandler.FromHandle(ctx, parent); err != nil {
		t.Fatalf("parent handle was evicted despite child access re-pinning it: %v", err)
	}
}

func TestFromHandleRepinsRoot(t *testing.T) {
	mem := memfs.New()
	handler := NewNullAuthHandler(mem)
	cacheHandler := NewCachingHandler(handler, 2).(*CachingHandler)
	ctx := t.Context()

	root := cacheHandler.ToHandle(ctx, mem, nil)
	child := cacheHandler.ToHandle(ctx, mem, []string{"file.txt"})

	if _, _, err := cacheHandler.FromHandle(ctx, child); err != nil {
		t.Fatalf("FromHandle(child) failed: %v", err)
	}

	cacheHandler.ToHandle(ctx, mem, []string{"other.txt"})

	if _, _, err := cacheHandler.FromHandle(ctx, root); err != nil {
		t.Fatalf("root handle was evicted despite child access re-pinning it: %v", err)
	}
}

// nonComparableFS is a valid billy.Filesystem whose concrete type is NOT
// comparable: it is a struct (value type) containing a map, so evaluating
// `iface == iface` on two of them panics ("comparing uncomparable type"). It
// models custom handlers that embed maps/slices. Only the methods exercised by
// ToHandle/FromHandle need real behavior; the rest delegate to an embedded
// memfs.
type nonComparableFS struct {
	billy.Filesystem
	tag map[string]int // makes the struct non-comparable
}

// TestCachingHandlerNonComparableFilesystem verifies ToHandle/FromHandle do not
// panic for a filesystem whose concrete type is non-comparable. Before the
// identity check was made comparability-safe, searchReverseCache did a bare
// `candidate.f == f`, which panics for such a type.
func TestCachingHandlerNonComparableFilesystem(t *testing.T) {
	fsA := nonComparableFS{Filesystem: memfs.New(), tag: map[string]int{"a": 1}}
	handler := NewNullAuthHandler(fsA)
	cacheHandler := NewCachingHandler(handler, 8).(*CachingHandler)
	ctx := t.Context()

	// First ToHandle populates the reverse cache; the second must consult it via
	// searchReverseCache (the bare == would panic here).
	h1 := cacheHandler.ToHandle(ctx, fsA, []string{"file.txt"})
	h2 := cacheHandler.ToHandle(ctx, fsA, []string{"file.txt"})
	if !bytes.Equal(h1, h2) {
		t.Fatal("expected the same handle for the same (fs, path)")
	}

	// A different instance of the same non-comparable type must be treated as a
	// distinct filesystem (different pointer/deep value), without panicking.
	fsB := nonComparableFS{Filesystem: memfs.New(), tag: map[string]int{"b": 2}}
	hB := cacheHandler.ToHandle(ctx, fsB, []string{"file.txt"})
	if bytes.Equal(h1, hB) {
		t.Fatal("distinct filesystems must not share a handle")
	}

	// FromHandle must resolve without panicking.
	if _, _, err := cacheHandler.FromHandle(ctx, h1); err != nil {
		t.Fatalf("FromHandle: %v", err)
	}
}
