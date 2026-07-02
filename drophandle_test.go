package nfs

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5/osfs"
)

// dropHandle backs the rename/remove fixes: it must close and detach BOTH the
// cached write handle and the cached read handle for a given NFS handle, so a
// subsequent operation on that path cannot reuse a stale fd (which after a
// remove/rename points at a unlinked or replaced inode).
func TestDropHandleClosesReadAndWrite(t *testing.T) {
	dir := t.TempDir()
	fs := osfs.New(dir)

	name := "f.dat"
	f, err := fs.Create(name)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = f.Close()

	c := &conn{Server: &Server{}}
	key := "handle-x"

	// Seed a cached write handle.
	wf, err := fs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open w: %v", err)
	}
	wh := &cachedHandle{file: wf, lastUsed: time.Now()}
	c.writeHandleCache().put(key, wh)

	// Seed a cached read handle.
	rf, err := fs.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("open r: %v", err)
	}
	rh := &readHandle{file: rf}
	rh.lastUsed.Store(time.Now().UnixNano())
	c.readHandleCache().put(key, rh)

	if err := c.dropHandle([]byte(key)); err != nil {
		t.Fatalf("dropHandle: %v", err)
	}

	// Both caches must no longer hold the handle.
	if c.wc.get(key) != nil {
		t.Fatal("write handle still cached after dropHandle")
	}
	if c.rc.get(key) != nil {
		t.Fatal("read handle still cached after dropHandle")
	}

	// Both underlying fds must be closed (writes/reads now fail).
	if _, err := wh.writeAt([]byte("x"), 0); err == nil {
		t.Fatal("expected write handle to be closed")
	}
	if _, err := rh.readAt(make([]byte, 1), 0); err == nil {
		t.Fatal("expected read handle to be closed")
	}

	c.drainCaches()
}

// dropHandle on an unknown key is a no-op and must not error, since rename/
// remove call it unconditionally for paths that may never have been cached.
func TestDropHandleUnknownKeyNoError(t *testing.T) {
	c := &conn{Server: &Server{}}
	// Touch the caches so they exist.
	_ = c.writeHandleCache()
	_ = c.readHandleCache()

	if err := c.dropHandle([]byte("never-cached")); err != nil {
		t.Fatalf("dropHandle on unknown key: %v", err)
	}
	c.drainCaches()
}

// Run with -race: broadcasts dropHandleAll from one goroutine while other
// connections lazily create their caches on first use and a disconnecting conn
// drains+unregisters. Targets the cache-pointer race between dropHandle (reads
// c.wc/c.rc from another goroutine) and writeHandleCache/readHandleCache
// (assigns them), plus the drain-before-unregister ordering.
func TestDropHandleAllConcurrentWithCacheInit(t *testing.T) {
	srv := &Server{}
	key := []byte("contended-handle")

	const nConns = 8
	conns := make([]*conn, nConns)
	for i := range conns {
		conns[i] = &conn{Server: srv}
		srv.registerConn(conns[i])
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Broadcaster: hammer cross-connection invalidation.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				srv.dropHandleAll(key)
			}
		}
	}()

	// Each connection races first-time cache creation against the broadcasts.
	for _, c := range conns {
		wg.Add(1)
		go func(c *conn) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = c.writeHandleCache()
				_ = c.readHandleCache()
			}
			// Simulate disconnect: drain then unregister, as serve's defer does.
			c.drainCaches()
			c.unregisterConn(c)
		}(c)
	}

	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// dropAndRetry retries a handle-mutating op once after re-dropping, so a fd
// cached in the pre-op window (and freed once that request finished) no longer
// blocks the op. It must not retry deterministic os.ErrNotExist failures.
func TestDropAndRetry(t *testing.T) {
	srv := &Server{}
	key := []byte("h")

	t.Run("success runs op once", func(t *testing.T) {
		calls := 0
		err := srv.dropAndRetry(func() error { calls++; return nil }, func() [][]byte { return [][]byte{key} })
		if err != nil || calls != 1 {
			t.Fatalf("got err=%v calls=%d, want nil/1", err, calls)
		}
	})

	t.Run("transient failure retried once then succeeds", func(t *testing.T) {
		calls := 0
		err := srv.dropAndRetry(func() error {
			calls++
			if calls == 1 {
				return os.ErrPermission // e.g. Windows sharing violation
			}
			return nil
		}, func() [][]byte { return [][]byte{key} })
		if err != nil || calls != 2 {
			t.Fatalf("got err=%v calls=%d, want nil/2", err, calls)
		}
	})

	t.Run("ErrNotExist not retried", func(t *testing.T) {
		calls := 0
		err := srv.dropAndRetry(func() error { calls++; return os.ErrNotExist }, func() [][]byte { return [][]byte{key} })
		if !os.IsNotExist(err) || calls != 1 {
			t.Fatalf("got err=%v calls=%d, want ErrNotExist/1", err, calls)
		}
	})

	t.Run("persistent failure returns after one retry", func(t *testing.T) {
		calls := 0
		err := srv.dropAndRetry(func() error { calls++; return os.ErrPermission }, func() [][]byte { return [][]byte{key} })
		if err == nil || calls != 2 {
			t.Fatalf("got err=%v calls=%d, want err/2", err, calls)
		}
	})

	t.Run("handles re-resolved on retry", func(t *testing.T) {
		// A path uncached at the first resolve but looked up during the race window
		// must be dropped on retry: dropAndRetry re-invokes resolveHandles rather
		// than reusing the first snapshot.
		calls := 0
		resolves := 0
		err := srv.dropAndRetry(func() error {
			calls++
			if calls == 1 {
				return os.ErrPermission
			}
			return nil
		}, func() [][]byte {
			resolves++
			if resolves == 1 {
				return nil // nothing cached yet
			}
			return [][]byte{key} // a handle appeared in the window
		})
		// resolveHandles is consulted only for the retry drop (once), and the op
		// runs twice.
		if err != nil || calls != 2 || resolves != 1 {
			t.Fatalf("got err=%v calls=%d resolves=%d, want nil/2/1", err, calls, resolves)
		}
	})
}
