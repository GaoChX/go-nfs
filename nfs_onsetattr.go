package nfs

import (
	"bytes"
	"context"
	"os"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

func onSetAttr(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = wccDataErrorFormatter
	handle, err := xdr.ReadOpaque(w.req.Body)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}

	fs, path, err := userHandle.FromHandle(ctx, handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	attrs, err := ReadSetFileAttributes(w.req.Body)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}

	fullPath := fs.Join(path...)
	info, err := fs.Lstat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		return &NFSStatusError{NFSStatusAccess, err}
	}

	// see if there's a "guard"
	if guard, err := xdr.ReadUint32(w.req.Body); err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	} else if guard != 0 {
		// read the ctime.
		t := FileTime{}
		if err := xdr.Read(w.req.Body, &t); err != nil {
			return &NFSStatusError{NFSStatusInval, err}
		}
		attr := ToFileAttribute(info, fullPath)
		if t != attr.Ctime {
			return &NFSStatusError{NFSStatusNotSync, nil}
		}
	}

	if !billy.CapabilityCheck(fs, billy.WriteCapability) {
		return &NFSStatusError{NFSStatusROFS, os.ErrPermission}
	}

	// Drop any cached read/write fd for this file before changing attributes,
	// across all connections (nconnect/second client may hold their own fd).
	// Backends enforce permissions at open() time, not per write/read, so a
	// cached O_RDWR fd opened before a chmod would keep writing despite the new
	// mode; dropping forces the next WRITE/READ to reopen and be re-checked.
	// Done before Apply so a SETATTR truncate acts on the file after the cached
	// handle's pending unstable writes are flushed (closeFlush), not racing them.
	w.dropHandleAll(handle)

	changer := userHandle.Change(ctx, fs)
	if err := attrs.Apply(changer, fs, fs.Join(path...)); err != nil {
		// Already an nfsstatuserror
		return err
	}

	// Drop again after applying attributes: a concurrent READ/WRITE on another
	// connection may have cached a fresh fd (opened under the old mode/size) in
	// the window between the pre-Apply snapshot and here. Closing it forces the
	// next READ/WRITE to reopen and observe the new attributes.
	w.dropHandleAll(handle)

	preAttr := ToFileAttribute(info, fullPath).AsCache()

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WriteWcc(writer, preAttr, tryStat(fs, path)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}
