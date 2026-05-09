package rewrite

import (
	"github.com/twmb/franz-go/pkg/kmsg"
)

// MetadataRequestIn rewrites the topic names in a Metadata request to their
// upstream form by prepending the tenant prefix.
func MetadataRequestIn(prefix string, req *kmsg.MetadataRequest) {
	for i := range req.Topics {
		req.Topics[i].Topic = PrefixInPtr(prefix, req.Topics[i].Topic)
	}
}

// MetadataResponseOut rewrites a Metadata response so the client sees only
// topics belonging to the tenant (with the prefix stripped) and so all
// advertised brokers point at the proxy's externally-visible address.
//
// advertisedHost / advertisedPort are the host/port on which the client
// reached the proxy and on which all subsequent broker traffic must arrive.
// We collapse the upstream broker list to a single virtual broker — node id
// 0 — and rewrite every leader/replica/ISR id in the partition metadata to
// point at it. This is safe because the proxy fronts a single upstream
// cluster and forwards every byte unchanged at the protocol layer.
func MetadataResponseOut(prefix, advertisedHost string, advertisedPort int32, resp *kmsg.MetadataResponse) {
	rack := (*string)(nil)
	if len(resp.Brokers) > 0 {
		rack = resp.Brokers[0].Rack
	}
	resp.Brokers = []kmsg.MetadataResponseBroker{{
		NodeID: 0,
		Host:   advertisedHost,
		Port:   advertisedPort,
		Rack:   rack,
	}}
	resp.ControllerID = 0

	out := resp.Topics[:0]
	for _, t := range resp.Topics {
		if t.Topic == nil {
			continue
		}
		if !BelongsToTenant(prefix, *t.Topic) {
			continue
		}
		t.Topic = StripOutPtr(prefix, t.Topic)
		for j := range t.Partitions {
			p := &t.Partitions[j]
			p.Leader = 0
			for k := range p.Replicas {
				p.Replicas[k] = 0
			}
			for k := range p.ISR {
				p.ISR[k] = 0
			}
			for k := range p.OfflineReplicas {
				p.OfflineReplicas[k] = 0
			}
		}
		out = append(out, t)
	}
	resp.Topics = out
}
