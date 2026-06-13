package proxy

import (
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// These helpers render a compact, debug-only summary of the topic names a
// request carries upstream and the per-topic error codes a response carries
// back. They exist purely to diagnose topic-prefix translation issues and are
// only invoked when the logger is at debug level.

func derefStr(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

func dbgOffsetFetchReq(req *kmsg.OffsetFetchRequest) string {
	var b strings.Builder
	for i := range req.Topics {
		b.WriteString(req.Topics[i].Topic + " ")
	}
	for i := range req.Groups {
		g := &req.Groups[i]
		fmt.Fprintf(&b, "{group=%s:", g.Group)
		for j := range g.Topics {
			b.WriteString(" " + g.Topics[j].Topic)
		}
		b.WriteString("}")
	}
	return strings.TrimSpace(b.String())
}

func dbgOffsetFetchResp(resp *kmsg.OffsetFetchResponse) string {
	var b strings.Builder
	fmt.Fprintf(&b, "topErr=%d", resp.ErrorCode)
	for i := range resp.Topics {
		t := &resp.Topics[i]
		for j := range t.Partitions {
			fmt.Fprintf(&b, " %s/%d=err%d", t.Topic, t.Partitions[j].Partition, t.Partitions[j].ErrorCode)
		}
	}
	for i := range resp.Groups {
		g := &resp.Groups[i]
		fmt.Fprintf(&b, " {group=%s gErr=%d:", g.Group, g.ErrorCode)
		for j := range g.Topics {
			tt := &g.Topics[j]
			for k := range tt.Partitions {
				fmt.Fprintf(&b, " %s/%d=err%d", tt.Topic, tt.Partitions[k].Partition, tt.Partitions[k].ErrorCode)
			}
		}
		b.WriteString("}")
	}
	return b.String()
}

func dbgMetadataReq(req *kmsg.MetadataRequest) string {
	if req.Topics == nil {
		return "<all>"
	}
	var b strings.Builder
	for i := range req.Topics {
		b.WriteString(derefStr(req.Topics[i].Topic) + " ")
	}
	return strings.TrimSpace(b.String())
}

func dbgMetadataResp(resp *kmsg.MetadataResponse) string {
	var b strings.Builder
	for i := range resp.Topics {
		t := &resp.Topics[i]
		fmt.Fprintf(&b, "%s=err%d ", derefStr(t.Topic), t.ErrorCode)
	}
	return strings.TrimSpace(b.String())
}

func dbgFetchReq(req *kmsg.FetchRequest) string {
	var b strings.Builder
	for i := range req.Topics {
		fmt.Fprintf(&b, "%s/id=%x ", req.Topics[i].Topic, req.Topics[i].TopicID)
	}
	return strings.TrimSpace(b.String())
}

func dbgFetchResp(resp *kmsg.FetchResponse) string {
	var b strings.Builder
	for i := range resp.Topics {
		t := &resp.Topics[i]
		for j := range t.Partitions {
			fmt.Fprintf(&b, "%s/%d=err%d ", t.Topic, t.Partitions[j].Partition, t.Partitions[j].ErrorCode)
		}
	}
	return strings.TrimSpace(b.String())
}

func dbgListOffsetsReq(req *kmsg.ListOffsetsRequest) string {
	var b strings.Builder
	for i := range req.Topics {
		b.WriteString(req.Topics[i].Topic + " ")
	}
	return strings.TrimSpace(b.String())
}

func dbgListOffsetsResp(resp *kmsg.ListOffsetsResponse) string {
	var b strings.Builder
	for i := range resp.Topics {
		t := &resp.Topics[i]
		for j := range t.Partitions {
			fmt.Fprintf(&b, "%s/%d=err%d ", t.Topic, t.Partitions[j].Partition, t.Partitions[j].ErrorCode)
		}
	}
	return strings.TrimSpace(b.String())
}
