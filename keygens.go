package nfs

// keyGens tracks a per-key invalidation generation for the open-then-publish
// race in the write and read handle caches, replacing a single cache-wide
// counter. A cache-wide counter had a false-rejection flaw: invalidating any
// key (even an absent or unrelated one) bumped the shared generation, so a
// concurrent open of a DIFFERENT file could have its publish refused and, after
// the bounded retries, surface a spurious ESTALE/EIO. Keying the generation by
// handle confines an invalidation to that handle.
//
// Lifecycle / memory bound: an entry exists only while at least one open for
// that key is in flight (sampled but not yet resolved). sample increments an
// outstanding-open refcount; resolve decrements it and deletes the entry when it
// reaches zero. So the map is bounded by the number of concurrent in-flight
// opens (≤ the worker pool size), not by the number of files ever invalidated —
// no unbounded growth from a long run of REMOVE/RENAME.
//
// Correctness of the absent-key case (a file removed while an open races): the
// racing open's sample creates the entry BEFORE its OpenFile, so a subsequent
// invalidate under the same lock bumps that entry and the open's fresh check
// rejects the now-stale fd. An invalidate that finds no entry means no open is
// in flight, so there is nothing to reject and no entry need be created. sample,
// invalidate, and fresh must all be called under the owning cache's lock, so the
// increment/bump/compare are atomic with the cache's own entries map operations.
type keyGens struct {
	m map[string]*keyGen
}

type keyGen struct {
	gen  uint64 // bumped by each invalidate while opens are in flight
	open int    // outstanding sample calls not yet resolve'd
}

func newKeyGens() *keyGens {
	return &keyGens{m: make(map[string]*keyGen)}
}

// sample records the start of an open for key and returns the generation to pass
// to fresh after the open completes. Must be paired with exactly one resolve.
func (k *keyGens) sample(key string) uint64 {
	kg := k.m[key]
	if kg == nil {
		kg = &keyGen{}
		k.m[key] = kg
	}
	kg.open++

	return kg.gen
}

// fresh reports whether no invalidation for key occurred since sample returned
// gen, i.e. the opened fd may still be published. It does NOT resolve the open;
// the caller calls resolve exactly once regardless of the result.
func (k *keyGens) fresh(key string, gen uint64) bool {
	kg := k.m[key]
	if kg == nil {
		// The entry is created by sample and only removed by resolve, so it must
		// still exist here for a correctly paired sample/fresh/resolve. Treat a
		// missing entry conservatively as "not fresh".
		return false
	}

	return kg.gen == gen
}

// resolve ends the open started by sample, dropping the entry once no opens
// remain outstanding for key (the memory bound).
func (k *keyGens) resolve(key string) {
	kg := k.m[key]
	if kg == nil {
		return
	}
	kg.open--
	if kg.open <= 0 {
		delete(k.m, key)
	}
}

// invalidate bumps key's generation so any in-flight open (which called sample
// before its OpenFile) will fail fresh and not publish an fd opened under the
// now-stale attributes. When no open is in flight (no entry), there is nothing
// to reject, so no entry is created — keeping the map bounded.
func (k *keyGens) invalidate(key string) {
	if kg := k.m[key]; kg != nil {
		kg.gen++
	}
}
