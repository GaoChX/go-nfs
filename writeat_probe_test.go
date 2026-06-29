package nfs_test

import (
	"os"
	"testing"

	billy "github.com/go-git/go-billy/v5"
	osfs "github.com/go-git/go-billy/v5/osfs"
)

// writerAtProbe is the exact capability cachedHandle.writeAt asserts to take its
// concurrent positional-write fast path. Keep it identical to the unexported
// nfs.writerAt interface in handlecache.go.
type writerAtProbe interface {
	WriteAt([]byte, int64) (int, error)
}

// ProbeWriteAtCapability reports whether the billy.File that fs.OpenFile returns
// for a writable file exposes WriteAt. This is the precise condition that
// decides whether the per-file concurrent-write fast path engages or falls back
// to serialized Seek+Write.
//
// To check a production backend, construct its billy.Filesystem and call this:
//
//	if !nfs_test.ProbeWriteAtCapability(myJuiceFSBilly, "probe.tmp") {
//	    // writes to a single file will serialize on the handle lock
//	}
//
// A common trap: a wrapper that embeds the billy.File *interface* (rather than a
// concrete *os.File) does not promote WriteAt, so the capability is hidden even
// when the real backend supports pwrite. osfs hits exactly this.
func ProbeWriteAtCapability(fs billy.Filesystem, name string) bool {
	f, err := fs.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return false
	}
	defer f.Close()
	_, ok := f.(writerAtProbe)

	return ok
}

// TestWriteAtCapability_osfs documents that the stock osfs backend does NOT
// expose WriteAt through its chroot wrapper, so the concurrent-write fast path
// is a no-op there. This is a regression guard for the finding, not a desired
// state: if a future go-billy promotes WriteAt, this test will flag the change.
func TestWriteAtCapability_osfs(t *testing.T) {
	fs := osfs.New(t.TempDir())
	got := ProbeWriteAtCapability(fs, "probe.tmp")
	if got {
		t.Log("osfs now exposes WriteAt: the per-file concurrent-write fast path engages")
	} else {
		t.Log("osfs does NOT expose WriteAt (chroot wrapper hides it): writes to one file serialize on Seek+Write")
	}
	// Pin the current v5.6.0 behavior so a change is noticed deliberately.
	if got {
		t.Errorf("osfs unexpectedly exposes WriteAt now; update the no-op note on the WriteAt commit")
	}
}
