package nfs

import (
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
)

// The core of the open-then-publish race fix: a handle opened before a
// concurrent invalidation (which bumps gen) must be refused by putIfFresh, even
// though nothing was cached when the invalidation ran. Otherwise an fd opened
// under the old mode/size gets cached AFTER a chmod/truncate dropped the handle,
// and later writes reuse it — bypassing the new permissions or re-extending past
// the truncate.
func TestWriteCachePutIfFreshRefusedAfterInvalidate(t *testing.T) {
	c := newWriteCache(nil)
	defer c.Close()

	key := "race-handle16!!!"

	// A WRITE samples the generation just before opening its fd.
	gen := c.sampleGen()

	// A concurrent SETATTR/REMOVE/RENAME drops the (not-yet-cached) handle. The
	// fd is still on the writer's stack, so there is nothing in entries to remove
	// — but invalidate must still bump gen so the pending publish is refused.
	if h, done := c.invalidate(key); h != nil || done != nil {
		t.Fatalf("invalidate of empty cache returned a handle: %v", h)
	}

	// The writer now tries to publish the fd it opened under the old attributes.
	h := &cachedHandle{file: closeErrFile{}, lastUsed: time.Now()}
	if c.putIfFresh(key, h, gen) {
		t.Fatal("putIfFresh published a stale fd after an intervening invalidation")
	}
	if got := c.get(key); got != nil {
		t.Fatalf("stale handle must not be cached, got %v", got)
	}
}

// Without an intervening invalidation, putIfFresh publishes normally (the fix
// must not block the common no-race path).
func TestWriteCachePutIfFreshPublishesWhenFresh(t *testing.T) {
	c := newWriteCache(nil)
	defer c.Close()

	key := "fresh-handle16!!"
	gen := c.sampleGen()
	h := &cachedHandle{file: closeErrFile{}, lastUsed: time.Now()}
	if !c.putIfFresh(key, h, gen) {
		t.Fatal("putIfFresh refused a handle with no intervening invalidation")
	}
	if got := c.get(key); got != h {
		t.Fatalf("handle not cached: got %v want %v", got, h)
	}
}

// If another goroutine already published a handle for the key, putIfFresh must
// refuse (the caller reuses the winner's handle rather than replacing it).
func TestWriteCachePutIfFreshRefusedWhenOccupied(t *testing.T) {
	c := newWriteCache(nil)
	defer c.Close()

	key := "occupied-handle1"
	gen := c.sampleGen()
	winner := &cachedHandle{file: closeErrFile{}, lastUsed: time.Now()}
	c.put(key, winner)

	loser := &cachedHandle{file: closeErrFile{}, lastUsed: time.Now()}
	if c.putIfFresh(key, loser, gen) {
		t.Fatal("putIfFresh replaced an already-cached handle")
	}
	if got := c.get(key); got != winner {
		t.Fatalf("winner handle was displaced: got %v", got)
	}
}

// readCache mirrors the same guard for read-only fds: a fd opened before an
// invalidation must not be cached, or a later READ would serve pre-chmod/
// pre-truncate bytes through it.
func TestReadCachePutIfFreshRefusedAfterInvalidate(t *testing.T) {
	c := newReadCache()
	defer c.Close()

	key := "rd-race-handle1!"
	gen := c.sampleGen()

	if h := c.invalidate(key); h != nil {
		t.Fatalf("invalidate of empty read cache returned a handle: %v", h)
	}

	h := &readHandle{file: openTempReadFile(t)}
	if c.putIfFresh(key, h, gen) {
		t.Fatal("read putIfFresh published a stale fd after an invalidation")
	}
	if got := c.get(key); got != nil {
		t.Fatalf("stale read handle must not be cached, got %v", got)
	}
	_ = h.close()
}

func TestReadCachePutIfFreshPublishesWhenFresh(t *testing.T) {
	c := newReadCache()
	defer c.Close()

	key := "rd-fresh-handle!"
	gen := c.sampleGen()
	h := &readHandle{file: openTempReadFile(t)}
	if !c.putIfFresh(key, h, gen) {
		t.Fatal("read putIfFresh refused a handle with no intervening invalidation")
	}
	if got := c.get(key); got != h {
		t.Fatalf("read handle not cached: got %v want %v", got, h)
	}
}

// openTempReadFile opens a real (empty) read fd so readHandle.close has a
// backing file to close.
func openTempReadFile(t *testing.T) billy.File {
	t.Helper()
	fs := osfs.New(t.TempDir())
	if f, err := fs.Create("r.dat"); err != nil {
		t.Fatalf("create: %v", err)
	} else {
		_ = f.Close()
	}
	fh, err := fs.Open("r.dat")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	return fh
}

// invalidate bumps the generation on every call (so successive racing opens are
// each refused), and a handle cached before the invalidate is detached.
func TestInvalidateBumpsGenAndDetaches(t *testing.T) {
	c := newWriteCache(nil)
	defer c.Close()

	key := "detach-handle16!"
	g0 := c.sampleGen()

	cached := &cachedHandle{file: closeErrFile{}, lastUsed: time.Now()}
	c.put(key, cached)

	h, done := c.invalidate(key)
	if h != cached {
		t.Fatalf("invalidate did not return the cached handle: got %v", h)
	}
	if done == nil {
		t.Fatal("invalidate of a cached handle must publish a commit marker")
	}
	c.commits.finish(key, done, nil)

	if c.sampleGen() == g0 {
		t.Fatal("invalidate must bump the generation")
	}
	if got := c.get(key); got != nil {
		t.Fatalf("handle must be detached after invalidate, got %v", got)
	}
}
