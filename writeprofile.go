package nfs

import (
	"expvar"
	"sync/atomic"
)

// writeProfile accumulates timing of the WRITE hot path so a live server can
// report where each WRITE spends its time without a full pprof capture. All
// counters are nanoseconds / counts via atomics (no lock on the hot path) and
// are published under expvar key "nfs_write_profile".
//
// It exists to settle, with measured data, whether the WRITE bottleneck is the
// backend write syscall (e.g. a FUSE/JuiceFS write blocking on its writeback
// cache) or time spent inside go-nfs. Enable by importing net/http/pprof or
// just reading expvar; disabled-cost is a handful of atomic adds per WRITE.
type writeProfileT struct {
	writes      atomic.Int64 // number of onWrite calls timed
	backendNs   atomic.Int64 // cumulative ns in the backing WriteAt/Write syscall
	backendMax  atomic.Int64 // slowest single backend write seen (ns)
	totalNs     atomic.Int64 // cumulative ns in onWrite end to end
	bytesWrote  atomic.Int64 // total bytes written
	syncs       atomic.Int64 // number of Sync() calls (stable writes/COMMIT)
	syncNs      atomic.Int64 // cumulative ns in Sync()
}

var writeProfile writeProfileT

// recordBackendWrite adds one backend write sample (the WriteAt/Write syscall to
// the backing FS).
func (p *writeProfileT) recordBackendWrite(ns int64, n int) {
	p.backendNs.Add(ns)
	p.bytesWrote.Add(int64(n))
	for {
		old := p.backendMax.Load()
		if ns <= old || p.backendMax.CompareAndSwap(old, ns) {
			break
		}
	}
}

func (p *writeProfileT) recordSync(ns int64) {
	p.syncs.Add(1)
	p.syncNs.Add(ns)
}

func init() {
	expvar.Publish("nfs_write_profile", expvar.Func(func() any {
		writes := writeProfile.writes.Load()
		backend := writeProfile.backendNs.Load()
		total := writeProfile.totalNs.Load()
		syncs := writeProfile.syncs.Load()
		syncNs := writeProfile.syncNs.Load()
		avg := func(sum, cnt int64) float64 {
			if cnt == 0 {
				return 0
			}
			return float64(sum) / float64(cnt) / 1e6 // ms
		}

		return map[string]any{
			"writes":             writes,
			"avg_backend_ms":     avg(backend, writes),
			"avg_total_ms":       avg(total, writes),
			"max_backend_ms":     float64(writeProfile.backendMax.Load()) / 1e6,
			"backend_frac":       fracOf(backend, total),
			"bytes":              writeProfile.bytesWrote.Load(),
			"syncs":              syncs,
			"avg_sync_ms":        avg(syncNs, syncs),
		}
	}))
}

func fracOf(part, whole int64) float64 {
	if whole == 0 {
		return 0
	}
	return float64(part) / float64(whole)
}
