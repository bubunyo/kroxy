package protocol

import (
	"encoding/binary"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// AppendResponseHeader appends a response header to dst. headerVersion 0 is
// just correlation_id. headerVersion 1 adds an empty tagged-field section
// (a single zero byte for "no tags").
func AppendResponseHeader(dst []byte, correlationID int32, headerVersion int8) []byte {
	var cid [4]byte
	binary.BigEndian.PutUint32(cid[:], uint32(correlationID))
	dst = append(dst, cid[:]...)
	if headerVersion >= 1 {
		dst = append(dst, 0) // empty tagged fields varint
	}
	return dst
}

// EncodeResponse builds a complete response frame body (header + body) for
// the given Response, correlation ID and API key/version.
func EncodeResponse(resp kmsg.Response, correlationID int32, apiKey, apiVersion int16) []byte {
	hv := ResponseHeaderVersion(apiKey, apiVersion)
	out := AppendResponseHeader(nil, correlationID, hv)
	return resp.AppendTo(out)
}
