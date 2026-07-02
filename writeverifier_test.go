package nfs

import (
	"errors"
	"testing"
	"time"
)

// A configured non-zero Server.ID must be used as the initial write verifier,
// preserving the historical behavior where WRITE/COMMIT replies returned
// Server.ID. Only a zero-value ID falls back to a random seed.
func TestWriteVerifierSeedsFromServerID(t *testing.T) {
	id := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	srv := &Server{ID: id}

	if got := srv.currentWriteVerifier(); got != id {
		t.Fatalf("initial verifier %x, want configured Server.ID %x", got, id)
	}
	// A rotation still moves it off the configured ID (durability loss → replay).
	srv.rotateWriteVerifier()
	if got := srv.currentWriteVerifier(); got == id {
		t.Fatal("verifier must change after rotation even when seeded from ID")
	}
}

// A zero-value Server.ID seeds a random (non-zero) verifier rather than handing
// out the all-zero value.
func TestWriteVerifierRandomWhenNoServerID(t *testing.T) {
	srv := &Server{}
	if got := srv.currentWriteVerifier(); got == ([8]byte{}) {
		t.Fatal("zero Server.ID must seed a non-zero random verifier")
	}
}

// The write verifier must be stable across reads within one server instance and
// non-zero (RFC 1813: constant during a single server instance). A zero-value
// Server lazily seeds it, so repeated reads return the same non-zero value.
func TestWriteVerifierStableAndNonZero(t *testing.T) {
	srv := &Server{}

	first := srv.currentWriteVerifier()
	if first == ([8]byte{}) {
		t.Fatal("write verifier must be seeded non-zero")
	}
	for i := 0; i < 5; i++ {
		if got := srv.currentWriteVerifier(); got != first {
			t.Fatalf("verifier changed without a loss: %x != %x", got, first)
		}
	}
}

// Rotating the verifier (on a durability loss) must yield a different, still
// non-zero value, so a client comparing its WRITE-reply verifier against a later
// COMMIT-reply verifier detects the loss and replays.
func TestWriteVerifierRotatesOnDemand(t *testing.T) {
	srv := &Server{}
	before := srv.currentWriteVerifier()

	srv.rotateWriteVerifier()

	after := srv.currentWriteVerifier()
	if after == before {
		t.Fatalf("verifier did not change after rotation: %x", after)
	}
	if after == ([8]byte{}) {
		t.Fatal("rotated verifier must remain non-zero")
	}
}

// The end-to-end contract: when a still-dirty handle's close-flush fails, the
// mount-wide tracker records the loss AND the server rotates its write verifier,
// so every client with outstanding unstable writes is told to replay — not just
// the one file whose COMMIT consumes the per-handle error.
func TestVerifierRotatesWhenDirtyFlushLost(t *testing.T) {
	srv := &Server{}
	c := &conn{Server: srv}
	srv.registerConn(c)
	defer c.drainCaches()

	// Seed the verifier so we can detect a change.
	before := srv.currentWriteVerifier()

	key := "lossy-handle16!!"
	flushErr := errors.New("ENOSPC")
	c.writeHandleCache().put(key, &cachedHandle{
		file: closeErrFile{err: flushErr}, dirty: true, lastUsed: time.Now(),
	})

	// A handle-mutating drop closes the dirty handle; its failed close-flush is a
	// lost-data event that must rotate the verifier.
	srv.dropHandleAll([]byte(key))

	if got := srv.currentWriteVerifier(); got == before {
		t.Fatalf("verifier must rotate after a lost dirty flush, still %x", got)
	}
	// And the per-handle error is still surfaced to a COMMIT.
	if got := srv.commitsTracker().wait(key); got != flushErr {
		t.Fatalf("COMMIT got %v, want %v", got, flushErr)
	}
}

// A clean handle whose close fails loses nothing unstable, so it must NOT rotate
// the verifier (no client needs to replay).
func TestVerifierStableWhenCleanCloseFails(t *testing.T) {
	srv := &Server{}
	c := &conn{Server: srv}
	srv.registerConn(c)
	defer c.drainCaches()

	before := srv.currentWriteVerifier()

	key := "clean-handle16!!"
	c.writeHandleCache().put(key, &cachedHandle{
		file: closeErrFile{err: errors.New("EIO")}, dirty: false, lastUsed: time.Now(),
	})
	srv.dropHandleAll([]byte(key))

	if got := srv.currentWriteVerifier(); got != before {
		t.Fatalf("verifier must not rotate for a clean close failure: %x != %x", got, before)
	}
}

// tracker.record (drain-time loss with no in-flight marker) must also fire the
// loss callback, so a verifier rotation happens even on the record path.
func TestTrackerRecordFiresLossCallback(t *testing.T) {
	tr := newCommitTracker()
	var fired int
	tr.onLoss = func() { fired++ }

	tr.record("h", errors.New("EIO"))
	if fired != 1 {
		t.Fatalf("record must fire onLoss once, got %d", fired)
	}
	// A nil error is not a loss and must not fire.
	tr.record("h", nil)
	if fired != 1 {
		t.Fatalf("nil record must not fire onLoss, got %d", fired)
	}
}

// finish with a non-nil error fires the loss callback; a successful flush does not.
func TestTrackerFinishFiresLossOnlyOnError(t *testing.T) {
	tr := newCommitTracker()
	var fired int
	tr.onLoss = func() { fired++ }

	done := tr.begin("h")
	tr.finish("h", done, nil)
	if fired != 0 {
		t.Fatalf("clean finish must not fire onLoss, got %d", fired)
	}

	done = tr.begin("h")
	tr.finish("h", done, errors.New("ENOSPC"))
	if fired != 1 {
		t.Fatalf("lossy finish must fire onLoss once, got %d", fired)
	}
}
