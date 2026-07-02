package nfs_test

import (
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	billy "github.com/go-git/go-billy/v5"
)

// laggyFS wraps a billy.Filesystem and adds a fixed latency to every file write,
// simulating a high-latency backend like JuiceFS (object-store round-trips of
// milliseconds) on top of a fast local FS. It also tracks the maximum number of
// writes in flight at once, which (by Little's law: throughput = concurrency /
// latency) is what determines whether the server can saturate such a backend.
type laggyFS struct {
	billy.Filesystem
	writeLatency time.Duration
	inFlight     atomic.Int64
	maxInFlight  atomic.Int64
	writes       atomic.Int64
}

func newLaggyFS(inner billy.Filesystem, latency time.Duration) *laggyFS {
	return &laggyFS{Filesystem: inner, writeLatency: latency}
}

func (l *laggyFS) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	f, err := l.Filesystem.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	return &laggyFile{File: f, fs: l}, nil
}

func (l *laggyFS) Create(name string) (billy.File, error) {
	f, err := l.Filesystem.Create(name)
	if err != nil {
		return nil, err
	}

	return &laggyFile{File: f, fs: l}, nil
}

// laggyFile adds write latency and tracks concurrency on the shared laggyFS. It
// exposes WriteAt (which stock osfs files do not) so the go-nfs write-handle
// cache takes its concurrent positional-write path; positional writes are
// serviced by a locked Seek+Write on the inner file, but the latency sleep
// happens OUTSIDE that lock so independent writes overlap.
type laggyFile struct {
	billy.File
	fs     *laggyFS
	seekMu sync.Mutex
}

func (f *laggyFile) enter() {
	cur := f.fs.inFlight.Add(1)
	for {
		max := f.fs.maxInFlight.Load()
		if cur <= max || f.fs.maxInFlight.CompareAndSwap(max, cur) {
			break
		}
	}
	f.fs.writes.Add(1)
	time.Sleep(f.fs.writeLatency)
}

func (f *laggyFile) leave() { f.fs.inFlight.Add(-1) }

func (f *laggyFile) Write(p []byte) (int, error) {
	f.enter()
	defer f.leave()

	return f.File.Write(p)
}

func (f *laggyFile) WriteAt(p []byte, off int64) (int, error) {
	// Simulate the backend round-trip first, without holding any lock, so
	// concurrent positional writes to the same file actually overlap.
	f.enter()
	defer f.leave()

	// The inner osfs file has no WriteAt, so emulate it with a locked seek+write.
	// This lock is held only for the local (fast) syscalls, not the latency.
	f.seekMu.Lock()
	defer f.seekMu.Unlock()
	if _, err := f.File.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}

	return f.File.Write(p)
}
