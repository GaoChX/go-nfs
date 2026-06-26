package nfs

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// A COMMIT (wait) that arrives while an eviction flush is in flight must block
// until the flush result is known, then observe its error — never race past to
// a false success. This is the strong-consistency contract for the LRU/idle
// eviction window (P1).
func TestCommitTrackerWaitsForInFlightFlush(t *testing.T) {
	tr := newCommitTracker()
	key := "h"
	flushErr := errors.New("ENOSPC")

	// Eviction begins: marker published before the (slow) flush runs.
	done := tr.begin(key)

	waitErr := make(chan error, 1)
	go func() { waitErr <- tr.wait(key) }()

	// The COMMIT must still be blocked: no result yet.
	select {
	case e := <-waitErr:
		t.Fatalf("wait returned %v before flush finished; must block", e)
	case <-time.After(20 * time.Millisecond):
	}

	// Flush completes with a lost-data error; waiter must now observe it.
	tr.finish(key, done, flushErr)

	select {
	case e := <-waitErr:
		if e != flushErr {
			t.Fatalf("wait got %v, want %v", e, flushErr)
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not return after finish")
	}

	// The error is consumed once: a second COMMIT sees nothing.
	if e := tr.wait(key); e != nil {
		t.Fatalf("second wait got %v, want nil", e)
	}
}

// await blocks for in-flight flushes like wait, but does NOT consume the recorded
// error: a READ observing a lost-dirty close failure must not clear it, because
// the client's later COMMIT for the same writes still needs to surface it.
func TestCommitTrackerAwaitDoesNotConsume(t *testing.T) {
	tr := newCommitTracker()
	key := "h"
	flushErr := errors.New("ENOSPC")

	done := tr.begin(key)

	awaitErr := make(chan error, 1)
	go func() { awaitErr <- tr.await(key) }()

	// Blocked until the flush result is known.
	select {
	case e := <-awaitErr:
		t.Fatalf("await returned %v before flush finished; must block", e)
	case <-time.After(20 * time.Millisecond):
	}

	tr.finish(key, done, flushErr)

	select {
	case e := <-awaitErr:
		if e != flushErr {
			t.Fatalf("await got %v, want %v", e, flushErr)
		}
	case <-time.After(time.Second):
		t.Fatal("await did not return after finish")
	}

	// The error is NOT consumed: a subsequent await still sees it...
	if e := tr.await(key); e != flushErr {
		t.Fatalf("second await got %v, want %v (await must not consume)", e, flushErr)
	}
	// ...and the eventual COMMIT (wait) consumes it.
	if e := tr.wait(key); e != flushErr {
		t.Fatalf("COMMIT wait got %v, want %v", e, flushErr)
	}
	if e := tr.wait(key); e != nil {
		t.Fatalf("after COMMIT consumed, got %v, want nil", e)
	}
}


func TestCommitTrackerSuccessfulFlushNoError(t *testing.T) {
	tr := newCommitTracker()
	key := "h"

	done := tr.begin(key)
	tr.finish(key, done, nil)

	if e := tr.wait(key); e != nil {
		t.Fatalf("wait got %v, want nil", e)
	}
}

// record stashes a drain-time error with no in-flight marker; a later COMMIT
// (even on a different connection) consumes it.
func TestCommitTrackerRecordSurfacedOnce(t *testing.T) {
	tr := newCommitTracker()
	key := "h"
	drainErr := errors.New("EIO")

	tr.record(key, drainErr)

	if e := tr.wait(key); e != drainErr {
		t.Fatalf("wait got %v, want %v", e, drainErr)
	}
	if e := tr.wait(key); e != nil {
		t.Fatalf("second wait got %v, want nil (consumed once)", e)
	}
}

// Concurrent begin/finish for the same key with waiters must not deadlock or
// drop the final error. Run with -race.
func TestCommitTrackerConcurrent(t *testing.T) {
	tr := newCommitTracker()
	key := "h"

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			done := tr.begin(key)
			tr.finish(key, done, errors.New("x"))
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = tr.wait(key)
		}()
	}
	wg.Wait()
}

// Two evictions of the same key can be in flight at once (nconnect: two
// connections each flushing their own fd). wait must not return until BOTH have
// finished, and a later flush succeeding must not mask an earlier flush's
// lost-data error.
func TestCommitTrackerWaitsForAllInFlight(t *testing.T) {
	tr := newCommitTracker()
	key := "h"
	lostErr := errors.New("ENOSPC")

	slow := tr.begin(key) // earlier flush: will fail (lost data)
	fast := tr.begin(key) // later flush: will succeed

	waitErr := make(chan error, 1)
	go func() { waitErr <- tr.wait(key) }()

	// The fast flush completes successfully first.
	tr.finish(key, fast, nil)

	// wait must still be blocked: the slow flush is outstanding.
	select {
	case e := <-waitErr:
		t.Fatalf("wait returned %v while a flush is still in flight", e)
	case <-time.After(20 * time.Millisecond):
	}

	// Slow flush finishes with the lost-data error.
	tr.finish(key, slow, lostErr)

	select {
	case e := <-waitErr:
		if e != lostErr {
			t.Fatalf("wait got %v, want %v (earlier failure must not be masked)", e, lostErr)
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not return after all flushes finished")
	}
}
