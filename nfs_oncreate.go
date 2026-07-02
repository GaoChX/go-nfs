package nfs

import (
	"bytes"
	"context"
	"os"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

const (
	createModeUnchecked = 0
	createModeGuarded   = 1
	createModeExclusive = 2
)

func onCreate(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = wccDataErrorFormatter
	obj := DirOpArg{}
	err := xdr.Read(w.req.Body, &obj)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	how, err := xdr.ReadUint32(w.req.Body)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	var attrs *SetFileAttributes
	if how == createModeUnchecked || how == createModeGuarded {
		sattr, err := ReadSetFileAttributes(w.req.Body)
		if err != nil {
			return &NFSStatusError{NFSStatusInval, err}
		}
		attrs = sattr
	} else if how == createModeExclusive {
		// read createverf3
		var verf [8]byte
		if err := xdr.Read(w.req.Body, &verf); err != nil {
			return &NFSStatusError{NFSStatusInval, err}
		}
		Log.Errorf("failing create to indicate lack of support for 'exclusive' mode.")
		// TODO: support 'exclusive' mode.
		return &NFSStatusError{NFSStatusNotSupp, os.ErrPermission}
	} else {
		// invalid
		return &NFSStatusError{NFSStatusNotSupp, os.ErrInvalid}
	}

	fs, path, err := userHandle.FromHandle(ctx, obj.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	if !billy.CapabilityCheck(fs, billy.WriteCapability) {
		return &NFSStatusError{NFSStatusROFS, os.ErrPermission}
	}

	if len(string(obj.Filename)) > PathNameMax {
		return &NFSStatusError{NFSStatusNameTooLong, nil}
	}

	newFile := append(path, string(obj.Filename))
	newFilePath := fs.Join(newFile...)
	fileExisted := false
	if s, err := fs.Stat(newFilePath); err == nil {
		if s.IsDir() {
			return &NFSStatusError{NFSStatusExist, nil}
		}
		if how == createModeGuarded {
			return &NFSStatusError{NFSStatusExist, os.ErrPermission}
		}
		fileExisted = true
	} else {
		if s, err := fs.Stat(fs.Join(path...)); err != nil {
			return &NFSStatusError{NFSStatusAccess, err}
		} else if !s.IsDir() {
			return &NFSStatusError{NFSStatusNotDir, nil}
		}
	}

	// Resolve the handle for an EXISTING file up front so we can drop any cached
	// fd before truncating it. For a brand-new file the path does not exist yet,
	// so handlers that derive handles from file metadata (e.g. device+inode, which
	// the Handler docs explicitly allow) could not form a valid handle here — mint
	// it only after fs.Create below, as the pre-cache code did.
	//
	// In unchecked mode CREATE on an existing file is a truncating overwrite
	// (fs.Create is O_CREATE|O_TRUNC|O_RDWR). A cached read/write fd for this
	// path — possibly on another connection (nconnect) — would then be stale:
	// its pending unstable writes could be flushed AFTER the truncate and
	// resurrect content, a cached O_RDWR fd would bypass the new attributes
	// (perms are checked at open), and Windows-like backends refuse to truncate
	// an open file. Drop across all connections before truncating, with
	// dropAndRetry guarding a racing READ/WRITE that re-caches an fd in the
	// window. New files have no prior cached fd, so this only matters when the
	// file already existed.
	var fp []byte
	var retryHandles [][]byte
	if fileExisted {
		fp = userHandle.ToHandle(ctx, fs, newFile)
		retryHandles = [][]byte{fp}
		w.dropHandleAll(fp)
	}

	var file billy.File
	err = w.Server.dropAndRetry(func() error {
		f, cerr := fs.Create(newFilePath)
		if cerr != nil {
			return cerr
		}
		file = f
		return nil
	}, func() [][]byte { return retryHandles })
	if err != nil {
		Log.Errorf("Error Creating: %v", err)
		return &NFSStatusError{NFSStatusAccess, err}
	}
	if err := file.Close(); err != nil {
		Log.Errorf("Error Creating: %v", err)
		return &NFSStatusError{NFSStatusAccess, err}
	}

	// The file now exists; mint its handle if we have not already (new file).
	if fp == nil {
		fp = userHandle.ToHandle(ctx, fs, newFile)
	}

	// Drop again after the truncate: a concurrent READ/WRITE on another
	// connection may have cached a fresh fd (over the pre-truncate file) in the
	// window between the pre-Create drop and here, so the next access reopens
	// against the truncated file.
	if fileExisted {
		w.dropHandleAll(fp)
	}

	changer := userHandle.Change(ctx, fs)
	if err := attrs.Apply(changer, fs, newFilePath); err != nil {
		Log.Errorf("Error applying attributes: %v\n", err)
		return &NFSStatusError{NFSStatusIO, err}
	}

	// Drop again after applying attributes, like SETATTR: a concurrent READ/WRITE
	// on another connection can cache a fresh O_RDWR fd (opened under the default
	// create mode) in the window between the create and Apply. Backends check
	// permissions at open() time, so without this such an fd would keep writing
	// despite the mode CREATE just set (e.g. 0444). Closing it forces the next
	// WRITE/READ to reopen and be re-checked against the applied attributes. This
	// applies to newly created files too, not just overwrites.
	w.dropHandleAll(fp)

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	// "handle follows"
	if err := xdr.Write(writer, uint32(1)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := xdr.Write(writer, fp); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WritePostOpAttrs(writer, tryStat(fs, []string{file.Name()})); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	// dir_wcc (we don't include pre_op_attr)
	if err := xdr.Write(writer, uint32(0)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WritePostOpAttrs(writer, tryStat(fs, path)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}
