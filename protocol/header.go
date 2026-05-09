package protocol

import (
	"encoding/binary"

	"github.com/pkg/errors"
)

// RequestHeader is the parsed Kafka request header. ClientID is decoded only
// when the header version includes it (v1+).
type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string

	// HeaderVersion is the negotiated request header version (0, 1 or 2)
	// that produced this struct. It is determined from APIKey + APIVersion.
	HeaderVersion int8

	// HeaderSize is the number of bytes the header consumed in the frame.
	HeaderSize int
}

// RequestHeaderVersion returns the request header version used for the given
// API key and version. This mirrors the table in the Kafka protocol spec.
//
// The default is v1 (with client_id). Flexible request versions use v2
// (client_id + tagged fields). ApiVersions is special: regardless of the
// request version, it always uses header v2 if flexible, but its response
// uses header v0 because broker compatibility predates flexible headers.
func RequestHeaderVersion(apiKey, apiVersion int16) int8 {
	if apiKey == ApiVersionsKey {
		if apiVersion >= 3 {
			return 2
		}
		return 1
	}
	if isFlexibleRequest(apiKey, apiVersion) {
		return 2
	}
	return 1
}

// ResponseHeaderVersion returns the response header version used for the
// given API key and version.
func ResponseHeaderVersion(apiKey, apiVersion int16) int8 {
	if apiKey == ApiVersionsKey {
		// ApiVersions response always uses header v0 for backwards compat.
		return 0
	}
	if isFlexibleRequest(apiKey, apiVersion) {
		return 1
	}
	return 0
}

// ParseRequestHeader decodes the header from the start of frame.
func ParseRequestHeader(frame []byte) (RequestHeader, error) {
	if len(frame) < 8 {
		return RequestHeader{}, errors.New("ParseRequestHeader: frame shorter than header")
	}
	h := RequestHeader{
		APIKey:        int16(binary.BigEndian.Uint16(frame[0:2])),
		APIVersion:    int16(binary.BigEndian.Uint16(frame[2:4])),
		CorrelationID: int32(binary.BigEndian.Uint32(frame[4:8])),
	}
	h.HeaderVersion = RequestHeaderVersion(h.APIKey, h.APIVersion)
	off := 8
	if h.HeaderVersion >= 1 {
		// client_id is a regular nullable string in v1, and a regular
		// nullable string in v2 (NOT compact). Per the spec, client_id
		// remains a non-compact nullable string even in flexible headers.
		if len(frame) < off+2 {
			return RequestHeader{}, errors.New("ParseRequestHeader: truncated client_id length")
		}
		clen := int16(binary.BigEndian.Uint16(frame[off : off+2]))
		off += 2
		if clen > 0 {
			if len(frame) < off+int(clen) {
				return RequestHeader{}, errors.New("ParseRequestHeader: truncated client_id")
			}
			h.ClientID = string(frame[off : off+int(clen)])
			off += int(clen)
		}
	}
	if h.HeaderVersion >= 2 {
		// tagged fields: an unsigned varint count, then count entries.
		// For requests we currently do not interpret tagged fields; we
		// only need to skip them.
		n, consumed, err := readUvarint(frame[off:])
		if err != nil {
			return RequestHeader{}, errors.Wrap(err, "ParseRequestHeader")
		}
		off += consumed
		for i := uint64(0); i < n; i++ {
			_, c1, err := readUvarint(frame[off:])
			if err != nil {
				return RequestHeader{}, errors.Wrap(err, "ParseRequestHeader")
			}
			off += c1
			length, c2, err := readUvarint(frame[off:])
			if err != nil {
				return RequestHeader{}, errors.Wrap(err, "ParseRequestHeader")
			}
			off += c2
			if len(frame) < off+int(length) {
				return RequestHeader{}, errors.New("ParseRequestHeader: truncated tagged field")
			}
			off += int(length)
		}
	}
	h.HeaderSize = off
	return h, nil
}

func readUvarint(buf []byte) (uint64, int, error) {
	var x uint64
	var s uint
	for i, b := range buf {
		if i >= 10 {
			return 0, 0, errors.New("uvarint overflow")
		}
		if b < 0x80 {
			return x | uint64(b)<<s, i + 1, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, 0, errors.New("uvarint truncated")
}
