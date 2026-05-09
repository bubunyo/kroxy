// Package protocol implements low-level Kafka wire-protocol helpers used by
// the proxy: length-prefixed framing and request/response header decoding.
package protocol

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

// MaxFrameSize is the maximum byte length of a single Kafka frame the proxy
// is willing to read or forward. 100 MiB matches common broker defaults.
const MaxFrameSize = 100 * 1024 * 1024

// ReadFrame reads a single length-prefixed Kafka frame from r and returns its
// body (without the 4-byte length prefix). It returns io.EOF only when the
// underlying reader is at a clean message boundary.
func ReadFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errors.Wrap(err, "ReadFrame")
		}
		return nil, err
	}
	size := binary.BigEndian.Uint32(hdr[:])
	if size == 0 {
		return nil, errors.New("ReadFrame: zero-length frame")
	}
	if size > MaxFrameSize {
		return nil, errors.Errorf("ReadFrame: frame too large: %d", size)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, errors.Wrap(err, "ReadFrame")
	}
	return buf, nil
}

// WriteFrame writes body as a length-prefixed Kafka frame to w.
func WriteFrame(w io.Writer, body []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := w.Write(hdr[:]); err != nil {
		return errors.Wrap(err, "WriteFrame")
	}
	if _, err := w.Write(body); err != nil {
		return errors.Wrap(err, "WriteFrame")
	}
	return nil
}
