package nfs

import "testing"

// TestKeyGensIsolation verifies that invalidating one key does not affect
// another key's generation or in-flight opens.
func TestKeyGensIsolation(t *testing.T) {
	kg := newKeyGens()

	genA := kg.sample("A")
	genB := kg.sample("B")

	// Invalidate A; B's gen must not change.
	kg.invalidate("A")
	if kg.fresh("A", genA) {
		t.Error("A should be stale after invalidate(A)")
	}
	if !kg.fresh("B", genB) {
		t.Error("B must still be fresh; invalidate(A) should not affect B")
	}

	kg.resolve("A")
	kg.resolve("B")
}

// TestKeyGensOpenRefcount verifies the entry is deleted once all opens resolve.
func TestKeyGensOpenRefcount(t *testing.T) {
	kg := newKeyGens()

	gen1 := kg.sample("K")
	gen2 := kg.sample("K") // second concurrent open

	if gen1 != gen2 {
		t.Error("concurrent samples of the same key must return the same gen")
	}
	if kg.m["K"] == nil {
		t.Fatal("entry must exist while opens are outstanding")
	}
	kg.resolve("K") // first open done
	if kg.m["K"] == nil {
		t.Error("entry must remain while one open is still outstanding")
	}
	kg.resolve("K") // second open done
	if kg.m["K"] != nil {
		t.Error("entry must be deleted once all opens resolve (memory bound)")
	}
}

// TestKeyGensInvalidateNoEntry verifies invalidate on an absent key (no
// in-flight open) is a no-op and does not create an entry.
func TestKeyGensInvalidateNoEntry(t *testing.T) {
	kg := newKeyGens()

	kg.invalidate("absent")
	if kg.m["absent"] != nil {
		t.Error("invalidate with no in-flight open must not create an entry (memory bound)")
	}
}

// TestKeyGensInvalidateDuringOpen verifies an invalidate arriving between
// sample and fresh bumps the gen so fresh returns false.
func TestKeyGensInvalidateDuringOpen(t *testing.T) {
	kg := newKeyGens()

	gen := kg.sample("K")
	// OpenFile would run here; a concurrent SETATTR calls invalidate:
	kg.invalidate("K")
	if kg.fresh("K", gen) {
		t.Error("fresh must return false when invalidate ran between sample and fresh")
	}
	kg.resolve("K")
}

// TestKeyGensResolveIdempotent verifies resolving a nonexistent key is safe.
func TestKeyGensResolveIdempotent(t *testing.T) {
	kg := newKeyGens()
	kg.resolve("never-sampled") // must not panic
}

// freshForTest exposes the per-key freshness check for tests that assert an
// in-flight open's generation was bumped by an invalidation.
func (c *writeCache) freshForTest(key string, gen uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.gens.fresh(key, gen)
}
