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
				_ = cacheHandler.ToHandle(mem, path)

				// Also access a shared path to increase contention
				sharedPath := []string{fmt.Sprintf("shared-%d.txt", j%10)}
				_ = cacheHandler.ToHandle(mem, sharedPath)
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
		handles[i] = cacheHandler.ToHandle(mem, path)
	}

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Writers - create new handles
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				path := []string{fmt.Sprintf("new-file-%d-%d.txt", id, j)}
				_ = cacheHandler.ToHandle(mem, path)
			}
		}(i)
	}

	// Readers - read existing handles
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				handle := handles[j%len(handles)]
				_, _, _ = cacheHandler.FromHandle(handle)
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
				handle := cacheHandler.ToHandle(mem, path)
				// Immediately invalidate some handles
				if j%3 == 0 {
					_ = cacheHandler.InvalidateHandle(mem, handle)
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
				_ = cacheHandler.ToHandle(mem, sharedPath)
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
// Run with: go test -race -run TestCachingHandlerReflectDeepEqualRace ./helpers/
func TestCachingHandlerReflectDeepEqualRace(t *testing.T) {
	// Create multiple filesystem instances to trigger reflect.DeepEqual comparisons
	const numFilesystems = 5
	filesystems := make([]billy.Filesystem, numFilesystems)
	for i := range filesystems {
		filesystems[i] = memfs.New()
	}

	handler := NewNullAuthHandler(filesystems[0])
	cacheHandler := NewCachingHandler(handler, 1024).(*CachingHandler)

	const numGoroutines = 10
	const numOperations = 100

	// Pre-populate cache with handles from different filesystems
	for _, fs := range filesystems {
		for j := 0; j < 10; j++ {
			path := []string{fmt.Sprintf("shared-%d.txt", j)}
			_ = cacheHandler.ToHandle(fs, path)
		}
	}

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Group 1: ToHandle calls triggering reflect.DeepEqual in searchReverseCache
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				fs := filesystems[j%numFilesystems]
				path := []string{fmt.Sprintf("shared-%d.txt", j%10)}
				_ = cacheHandler.ToHandle(fs, path)
			}
		}(i)
	}

	// Group 2: File operations modifying filesystem internal state
	// This races with reflect.DeepEqual reading that state
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			fs := filesystems[id%numFilesystems]
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
