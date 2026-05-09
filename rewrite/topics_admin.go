package rewrite

import "github.com/twmb/franz-go/pkg/kmsg"

const (
	configResourceTypeTopic int8 = 2
	configResourceTypeGroup int8 = 32
)

// CreateTopicsRequestIn prefixes every topic name to be created.
func CreateTopicsRequestIn(prefix string, req *kmsg.CreateTopicsRequest) {
	for i := range req.Topics {
		req.Topics[i].Topic = PrefixIn(prefix, req.Topics[i].Topic)
	}
}

// CreateTopicsResponseOut strips the tenant prefix from each returned topic
// name.
func CreateTopicsResponseOut(prefix string, resp *kmsg.CreateTopicsResponse) {
	for i := range resp.Topics {
		resp.Topics[i].Topic = StripOut(prefix, resp.Topics[i].Topic)
	}
}

// DeleteTopicsRequestIn prefixes every topic name to be deleted, both in the
// legacy v0-v5 TopicNames flat list and the v6+ per-topic struct form.
func DeleteTopicsRequestIn(prefix string, req *kmsg.DeleteTopicsRequest) {
	for i, n := range req.TopicNames {
		req.TopicNames[i] = PrefixIn(prefix, n)
	}
	for i := range req.Topics {
		req.Topics[i].Topic = PrefixInPtr(prefix, req.Topics[i].Topic)
	}
}

// DeleteTopicsResponseOut strips the tenant prefix from each returned topic
// name.
func DeleteTopicsResponseOut(prefix string, resp *kmsg.DeleteTopicsResponse) {
	for i := range resp.Topics {
		resp.Topics[i].Topic = StripOutPtr(prefix, resp.Topics[i].Topic)
	}
}

// DescribeConfigsRequestIn prefixes the resource name when it identifies a
// topic or group config; broker-level resources are passed through unchanged
// because the proxy presents a single virtual broker (node 0) to clients.
func DescribeConfigsRequestIn(prefix string, req *kmsg.DescribeConfigsRequest) {
	for i := range req.Resources {
		r := &req.Resources[i]
		switch int8(r.ResourceType) {
		case configResourceTypeTopic, configResourceTypeGroup:
			r.ResourceName = PrefixIn(prefix, r.ResourceName)
		}
	}
}

// DescribeConfigsResponseOut strips the tenant prefix from topic/group
// resource names and drops resources that do not belong to the tenant
// (defence in depth — the broker should only have returned what we asked
// for, but stripping foreign resources protects against broker bugs and
// future request shapes).
func DescribeConfigsResponseOut(prefix string, resp *kmsg.DescribeConfigsResponse) {
	out := resp.Resources[:0]
	for _, r := range resp.Resources {
		switch int8(r.ResourceType) {
		case configResourceTypeTopic, configResourceTypeGroup:
			if !BelongsToTenant(prefix, r.ResourceName) {
				continue
			}
			r.ResourceName = StripOut(prefix, r.ResourceName)
		}
		out = append(out, r)
	}
	resp.Resources = out
}
