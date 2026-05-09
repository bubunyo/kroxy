package rewrite

import "github.com/twmb/franz-go/pkg/kmsg"

// ProduceRequestIn rewrites topic names in a Produce request to upstream form.
func ProduceRequestIn(prefix string, req *kmsg.ProduceRequest) {
	for i := range req.Topics {
		req.Topics[i].Topic = PrefixIn(prefix, req.Topics[i].Topic)
	}
}

// ProduceResponseOut strips the tenant prefix from topic names in a Produce
// response.
func ProduceResponseOut(prefix string, resp *kmsg.ProduceResponse) {
	for i := range resp.Topics {
		resp.Topics[i].Topic = StripOut(prefix, resp.Topics[i].Topic)
	}
}

// FetchRequestIn rewrites topic names in a Fetch request to upstream form.
func FetchRequestIn(prefix string, req *kmsg.FetchRequest) {
	for i := range req.Topics {
		req.Topics[i].Topic = PrefixIn(prefix, req.Topics[i].Topic)
	}
}

// FetchResponseOut strips the tenant prefix from topic names in a Fetch
// response.
func FetchResponseOut(prefix string, resp *kmsg.FetchResponse) {
	for i := range resp.Topics {
		resp.Topics[i].Topic = StripOut(prefix, resp.Topics[i].Topic)
	}
}

// ListOffsetsRequestIn rewrites topic names in a ListOffsets request.
func ListOffsetsRequestIn(prefix string, req *kmsg.ListOffsetsRequest) {
	for i := range req.Topics {
		req.Topics[i].Topic = PrefixIn(prefix, req.Topics[i].Topic)
	}
}

// ListOffsetsResponseOut strips the tenant prefix from topic names in a
// ListOffsets response.
func ListOffsetsResponseOut(prefix string, resp *kmsg.ListOffsetsResponse) {
	for i := range resp.Topics {
		resp.Topics[i].Topic = StripOut(prefix, resp.Topics[i].Topic)
	}
}
