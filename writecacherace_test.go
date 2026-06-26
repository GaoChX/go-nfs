package nfs

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5/osfs"
)

// Regression for the LRU-victim data race: writeAt mutates h.lastUsed under
// h.mu while a concurrent write to a different file fills the cache and scans
// lastUsed to pick a victim. evictOldestLocked must read lastUsed under h.mu.
// Run with -race; without the fix this trips the detector.
func TestWriteCacheLRUVictimNoRace(t *testing.T) {
	fs := osfs.New(t.TempDir())
	c := newWriteCache(nil)
	defer c.Close()
	c.maxSize = 4

	const writers = 8
	const ops = 300
	var wg sync.WaitGroup
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				name := fmt.Sprintf("f-%d-%d.dat", id, j%16)
				key := name
				h := c.get(key)
				if h == nil {
					f, err := fs.Create(name)
					if err != nil {
						t.Errorf("create: %v", err)
						return
					}
					h = &cachedHandle{file: f, lastUsed: time.Now()}
					c.put(key, h) // may evict, scanning other handles' lastUsed
				}
				// writeAt mutates lastUsed under h.mu, racing the eviction scan.
				_, _ = h.writeAt([]byte("x"), 0)
			}
		}(i)
	}
	wg.Wait()
}

// Regression for the no-Sync stable-write durability hole: the handle a
// dataSync/fileSync WRITE synced can be concurrently closed (its failed close
// recorded on the tracker) and a fresh handle cached for the same key before the
// write's removeAndTrack runs. Closing that fresh (clean) handle says nothing
// about the original bytes' durability, so the stable WRITE must still consult
// the tracker and surface the original loss — its client will not send a COMMIT.
func TestStableWriteSurfacesReplacedHandleLoss(t *testing.T) {
	srv := &Server{}
	c := &conn{Server: srv}
	srv.registerConn(c)
	defer c.drainCaches()

	key := "replaced-handle1"
	wantErr := errors.New("ENOSPC")

	// Model the post-sync state: the original handle was concurrently closed with
	// a failed flush (recorded on the mount-wide tracker, as dropHandle would),
	// and a fresh CLEAN handle now occupies the key.
	srv.commitsTracker().record(key, wantErr)
	c.writeHandleCache().put(key, &cachedHandle{
		file: closeErrFile{}, dirty: false, lastUsed: time.Now(),
	})

	// The stable-write recovery path: removeAndTrack returns the fresh clean
	// handle; closing it is clean, but the tracker still holds the original loss.
	if dropped, done := c.writeHandleCache().removeAndTrack(key); dropped != nil {
		lost, cerr := dropped.evictFlush()
		srv.commitsTracker().finish(key, done, lostErr(lost, cerr))
	}
	// The unconditional tracker consult must surface the original ENOSPC.
	if got := srv.commitsTracker().wait(key); got != wantErr {
		t.Fatalf("stable WRITE got %v, want recorded original loss %v", got, wantErr)
	}
}
