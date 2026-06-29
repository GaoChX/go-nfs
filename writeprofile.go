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
	writes     atomic.Int64 // number of onWrite calls timed
	backendNs  atomic.Int64 // cumulative ns in the backing WriteAt/Write syscall
	backendMax atomic.Int64 // slowest single backend write seen (ns)
	totalNs    atomic.Int64 // cumulative ns in onWrite end to end
	bytesWrote atomic.Int64 // total bytes written
	syncs      atomic.Int64 // number of Sync() calls (stable writes/COMMIT/read flush)
	syncNs     atomic.Int64 // cumulative ns in Sync()

	// onWrite segment timings, to localize non-backend time.
	fromHandleNs  atomic.Int64 // cumulative ns in userHandle.FromHandle
	validateNs    atomic.Int64 // cumulative ns in capability checks, validation, path/count setup
	preStatNs     atomic.Int64 // cumulative ns building the pre-op wcc (fstat/path stat)
	postStatNs    atomic.Int64 // cumulative ns building the post-op attrs (fstat/path stat)
	decodeNs      atomic.Int64 // cumulative ns in xdr.Read decoding the request (incl. data)
	stableWriteNs atomic.Int64 // cumulative ns making dataSync/fileSync WRITEs durable
	replyNs       atomic.Int64 // cumulative ns building+queueing the reply
	cachedWriteNs atomic.Int64 // cumulative ns in cachedWrite (incl. backend write)

	unstableWrites atomic.Int64 // number of WRITE requests asking for UNSTABLE
	dataSyncWrites atomic.Int64 // number of WRITE requests asking for DATA_SYNC
	fileSyncWrites atomic.Int64 // number of WRITE requests asking for FILE_SYNC
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

func (p *writeProfileT) recordStability(stability writeStability) {
	switch stability {
	case unstable:
		p.unstableWrites.Add(1)
	case dataSync:
		p.dataSyncWrites.Add(1)
	case fileSync:
		p.fileSyncWrites.Add(1)
	}
}

func init() {
	expvar.Publish("nfs_write_profile", expvar.Func(func() any {
		writes := writeProfile.writes.Load()
		backend := writeProfile.backendNs.Load()
		total := writeProfile.totalNs.Load()
		syncs := writeProfile.syncs.Load()
		syncNs := writeProfile.syncNs.Load()
		fromHandle := writeProfile.fromHandleNs.Load()
		validate := writeProfile.validateNs.Load()
		preStat := writeProfile.preStatNs.Load()
		postStat := writeProfile.postStatNs.Load()
		decode := writeProfile.decodeNs.Load()
		stableWrite := writeProfile.stableWriteNs.Load()
		reply := writeProfile.replyNs.Load()
		cachedWrite := writeProfile.cachedWriteNs.Load()
		stableWrites := writeProfile.dataSyncWrites.Load() + writeProfile.fileSyncWrites.Load()
		accounted := fromHandle + validate + preStat + postStat + decode + stableWrite + reply + cachedWrite
		unaccounted := total - accounted
		if unaccounted < 0 {
			unaccounted = 0
		}
		avg := func(sum, cnt int64) float64 {
			if cnt == 0 {
				return 0
			}
			return float64(sum) / float64(cnt) / 1e6 // ms
		}

		return map[string]any{
			"writes":                        writes,
			"avg_backend_ms":                avg(backend, writes),
			"avg_total_ms":                  avg(total, writes),
			"avg_fromhandle_ms":             avg(fromHandle, writes),
			"avg_validate_ms":               avg(validate, writes),
			"avg_prestat_ms":                avg(preStat, writes),
			"avg_poststat_ms":               avg(postStat, writes),
			"avg_decode_ms":                 avg(decode, writes),
			"avg_write_stable_ms":           avg(stableWrite, stableWrites),
			"avg_write_stable_per_write_ms": avg(stableWrite, writes),
			"avg_reply_ms":                  avg(reply, writes),
			"avg_cachedwrite_ms":            avg(cachedWrite, writes),
			"avg_accounted_ms":              avg(accounted, writes),
			"avg_unaccounted_ms":            avg(unaccounted, writes),
			"max_backend_ms":                float64(writeProfile.backendMax.Load()) / 1e6,
			"backend_frac":                  fracOf(backend, total),
			"bytes":                         writeProfile.bytesWrote.Load(),
			"syncs":                         syncs,
			"avg_sync_ms":                   avg(syncNs, syncs),
			"unstable_writes":               writeProfile.unstableWrites.Load(),
			"datasync_writes":               writeProfile.dataSyncWrites.Load(),
			"filesync_writes":               writeProfile.fileSyncWrites.Load(),
		}
	}))
}

func fracOf(part, whole int64) float64 {
	if whole == 0 {
		return 0
	}
	return float64(part) / float64(whole)
}
