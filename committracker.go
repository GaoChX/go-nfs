package nfs

import "sync"

// commitTracker tracks the durability of unstable writes at mount (Server)
// scope, keyed by the opaque NFS file handle. The NFSv3 COMMIT contract is
// mount-wide: with nconnect a file's WRITEs and its COMMIT may land on
// different connections, so the per-connection writeCache is too narrow a scope
// to record "acknowledged but not-yet-durable" state. Two situations need a
// scope that any connection's COMMIT can consult:
//
//   - LRU/idle eviction (writeCache.put / sweepIdle): the victim handle is
//     removed from its cache's entries before its close-flush runs. In that
//     window a COMMIT for the victim on a peer connection would find no cached
//     handle and fall back to commitByPath, replying OK while the only fd
//     holding the buffered unstable data has not closed yet (or later fails to
//     close with ENOSPC/EIO).
//   - Connection drain (writeCache.Close): a connection disconnecting with dirty
//     handles has no future COMMIT of its own, but a peer connection may still
//     COMMIT the same file; a flush failure there must not be reported as OK.
//
// To honor the contract strictly (never reply OK for data that was lost), an
// eviction publishes a pending marker via begin *before* the handle leaves the
// cache (begin is called while holding writeCache.mu, atomically with the
// removal from entries, so any COMMIT that fails to find the handle is
// guaranteed to observe the marker). A COMMIT that arrives while a flush is in
// flight waits (wait) until *every* in-flight flush for the key has finished
// rather than racing past to commitByPath. When a flush finishes, finish
// records its error (if any) and removes its own marker.
//
// Multiple flushes for the same key can be in flight at once (nconnect: two
// connections each evicting their own fd for the file), so markers are tracked
// per-flush, not per-key: a later eviction never displaces an earlier flush's
// marker, and wait cannot return until the slowest of them completes.
//
// Locking: commitTracker.mu is a leaf lock. It is only ever held briefly and
// without blocking (begin/finish/record/wait take it, do map work, release).
// begin is non-blocking, so it is safe to call while holding writeCache.mu,
// establishing the lock order writeCache.mu → commitTracker.mu. The COMMIT path
// never holds writeCache.mu while calling wait, and a handle is never
// closed/flushed under either lock, so no cycle is possible. wait blocks on a
// channel with no lock held.
type commitTracker struct {
	mu sync.Mutex
	// pending maps a handle key to the set of channels for its in-flight eviction
	// flushes, each closed when that flush completes. A COMMIT seeing any pending
	// entry waits for all of them. Multiple concurrent evictions of the same key
	// each get their own channel so none is lost.
	pending map[string]map[chan struct{}]struct{}
	// errs maps a handle key to the flush error from a completed eviction whose
	// still-dirty data may have been lost. Consumed (and cleared) by the next
	// COMMIT for that key via wait.
	errs map[string]error
	// onLoss, if set, is invoked (without the tracker lock held) whenever a
	// non-nil loss is recorded — a still-dirty handle's close-flush failed, so
	// acknowledged UNSTABLE data may be gone. The Server wires this to rotate the
	// write verifier so every client with outstanding unstable writes replays,
	// not just the one whose COMMIT happens to consume this per-handle error. nil
	// for standalone trackers (tests) that have no server verifier to rotate.
	onLoss func()
}

func newCommitTracker() *commitTracker {
	return &commitTracker{
		pending: make(map[string]map[chan struct{}]struct{}),
		errs:    make(map[string]error),
	}
}

// begin publishes a pending marker for an in-flight eviction flush of key and
// returns the channel to close once the result is known (via finish). It is
// non-blocking and intended to be called while holding writeCache.mu, at the
// moment the handle is removed from the cache, so the window in which the
// handle is neither in the cache nor marked pending is closed. Concurrent
// flushes for the same key each add a distinct channel, so an earlier flush's
// marker is never displaced and wait observes all of them.
func (t *commitTracker) begin(key string) chan struct{} {
	ch := make(chan struct{})
	t.mu.Lock()
	set := t.pending[key]
	if set == nil {
		set = make(map[chan struct{}]struct{})
		t.pending[key] = set
	}
	set[ch] = struct{}{}
	t.mu.Unlock()

	return ch
}

// finish records the result of the eviction flush started with begin: a
// non-nil err (the close-flush of a still-dirty handle failed, so acknowledged
// unstable data may be lost) is stashed for the next COMMIT to surface. This
// flush's own marker is removed and ch is closed to wake any waiting COMMIT,
// which re-checks for other still-pending flushes.
func (t *commitTracker) finish(key string, ch chan struct{}, err error) {
	t.mu.Lock()
	if err != nil {
		t.errs[key] = err
	}
	if set := t.pending[key]; set != nil {
		delete(set, ch)
		if len(set) == 0 {
			delete(t.pending, key)
		}
	}
	t.mu.Unlock()

	close(ch)

	// A lost-dirty flush failure means acknowledged unstable data may be gone;
	// signal a server-wide verifier rotation outside the lock so all clients with
	// outstanding unstable writes replay (see onLoss).
	if err != nil {
		t.fireLoss()
	}
}

// wait blocks until every in-flight eviction flush for key has completed, then
// returns and clears any recorded flush error. Called by COMMIT before falling
// back to commitByPath (and before reporting success): pending flushes are
// awaited so their results are observed rather than raced past, and the error is
// consumed because COMMIT is the durability checkpoint that resolves it. Returns
// nil when nothing is pending or recorded.
func (t *commitTracker) wait(key string) error {
	return t.resolve(key, true)
}

// await is wait without consuming: it blocks for in-flight flushes and returns
// any recorded loss but leaves it in place. READ coherence uses this — a READ
// must observe a lost-dirty close failure (so it does not serve a fresh read-only
// fd's stale bytes as if the unstable write were durable), but unlike COMMIT it
// must not clear the error: the client's later COMMIT for those same writes still
// needs to surface it. Returns nil when nothing is pending or recorded.
func (t *commitTracker) await(key string) error {
	return t.resolve(key, false)
}

// resolve blocks until no eviction flush for key is in flight, then returns the
// recorded error, clearing it when consume is true. The pending-empty check and
// the errs read happen under one lock acquisition so a flush starting
// concurrently cannot slip a result past the caller.
func (t *commitTracker) resolve(key string, consume bool) error {
	for {
		t.mu.Lock()
		set := t.pending[key]
		if len(set) == 0 {
			err := t.errs[key]
			if consume {
				delete(t.errs, key)
			}
			t.mu.Unlock()

			return err
		}
		// Grab any one in-flight channel, wait for it outside the lock, then loop
		// to re-check (more may remain, or have been added).
		var ch chan struct{}
		for c := range set {
			ch = c
			break
		}
		t.mu.Unlock()
		<-ch
	}
}

// record stashes a flush error for key with no in-flight marker, for callers
// that have already completed the flush (e.g. a path that did not publish a
// begin marker). A nil error is ignored.
func (t *commitTracker) record(key string, err error) {
	if err == nil {
		return
	}
	t.mu.Lock()
	t.errs[key] = err
	t.mu.Unlock()

	t.fireLoss()
}

// fireLoss invokes the registered loss callback, if any, outside the tracker
// lock. Called whenever a non-nil lost-dirty error is recorded so the Server can
// rotate the write verifier.
func (t *commitTracker) fireLoss() {
	if t.onLoss != nil {
		t.onLoss()
	}
}
