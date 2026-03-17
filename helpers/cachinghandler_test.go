package helpers

import (
	"fmt"
	"sync"
	"testing"

	"github.com/go-git/go-billy/v5"
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
