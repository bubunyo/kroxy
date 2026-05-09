package rewrite_test

import (
	"testing"

	"github.com/bubunyo/kroxy/rewrite"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func strPtr(s string) *string { return &s }

func TestPrefixAndStrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		prefix string
		in     string
		want   string
	}{
		{name: "prefix non-empty", prefix: "tA.", in: "orders", want: "tA.orders"},
		{name: "prefix empty", prefix: "", in: "orders", want: "orders"},
		{name: "input empty", prefix: "tA.", in: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := rewrite.PrefixIn(tt.prefix, tt.in)
			assert.Equal(t, tt.want, got)
			back := rewrite.StripOut(tt.prefix, got)
			assert.Equal(t, tt.in, back)
		})
	}
}

func TestPrefixInPtr_NilSafe(t *testing.T) {
	t.Parallel()
	assert.Nil(t, rewrite.PrefixInPtr("tA.", nil))
	assert.Nil(t, rewrite.StripOutPtr("tA.", nil))
}

func TestBelongsToTenant(t *testing.T) {
	t.Parallel()
	assert.True(t, rewrite.BelongsToTenant("tA.", "tA.orders"))
	assert.False(t, rewrite.BelongsToTenant("tA.", "tB.orders"))
	assert.True(t, rewrite.BelongsToTenant("", "anything"))
}

func TestMetadata_RoundTrip(t *testing.T) {
	t.Parallel()

	const prefix = "tA."

	req := &kmsg.MetadataRequest{
		Topics: []kmsg.MetadataRequestTopic{
			{Topic: strPtr("orders")},
			{Topic: strPtr("payments")},
		},
	}
	rewrite.MetadataRequestIn(prefix, req)
	assert.Equal(t, "tA.orders", *req.Topics[0].Topic)
	assert.Equal(t, "tA.payments", *req.Topics[1].Topic)

	resp := &kmsg.MetadataResponse{
		Brokers: []kmsg.MetadataResponseBroker{
			{NodeID: 11, Host: "real-broker-1", Port: 9092},
			{NodeID: 12, Host: "real-broker-2", Port: 9092},
		},
		ControllerID: 11,
		Topics: []kmsg.MetadataResponseTopic{
			{Topic: strPtr("tA.orders"), Partitions: []kmsg.MetadataResponseTopicPartition{
				{Partition: 0, Leader: 11, Replicas: []int32{11, 12}, ISR: []int32{11, 12}},
			}},
			{Topic: strPtr("tB.secrets")},
			{Topic: strPtr("tA.payments")},
		},
	}
	rewrite.MetadataResponseOut(prefix, "kroxy.local", 9092, resp)

	require := assert.New(t)
	require.Len(resp.Brokers, 1)
	require.Equal(int32(0), resp.Brokers[0].NodeID)
	require.Equal("kroxy.local", resp.Brokers[0].Host)
	require.Equal(int32(9092), resp.Brokers[0].Port)
	require.Equal(int32(0), resp.ControllerID)

	require.Len(resp.Topics, 2, "tB.secrets must be filtered out")
	require.Equal("orders", *resp.Topics[0].Topic)
	require.Equal("payments", *resp.Topics[1].Topic)
	p := resp.Topics[0].Partitions[0]
	require.Equal(int32(0), p.Leader)
	require.Equal([]int32{0, 0}, p.Replicas)
	require.Equal([]int32{0, 0}, p.ISR)
}

func TestProduce_RoundTrip(t *testing.T) {
	t.Parallel()
	const prefix = "tA."

	req := &kmsg.ProduceRequest{Topics: []kmsg.ProduceRequestTopic{{Topic: "orders"}}}
	rewrite.ProduceRequestIn(prefix, req)
	assert.Equal(t, "tA.orders", req.Topics[0].Topic)

	resp := &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{{Topic: "tA.orders"}}}
	rewrite.ProduceResponseOut(prefix, resp)
	assert.Equal(t, "orders", resp.Topics[0].Topic)
}

func TestFetch_RoundTrip(t *testing.T) {
	t.Parallel()
	const prefix = "tA."

	req := &kmsg.FetchRequest{Topics: []kmsg.FetchRequestTopic{{Topic: "orders"}}}
	rewrite.FetchRequestIn(prefix, req)
	assert.Equal(t, "tA.orders", req.Topics[0].Topic)

	resp := &kmsg.FetchResponse{Topics: []kmsg.FetchResponseTopic{{Topic: "tA.orders"}}}
	rewrite.FetchResponseOut(prefix, resp)
	assert.Equal(t, "orders", resp.Topics[0].Topic)
}

func TestListOffsets_RoundTrip(t *testing.T) {
	t.Parallel()
	const prefix = "tA."

	req := &kmsg.ListOffsetsRequest{Topics: []kmsg.ListOffsetsRequestTopic{{Topic: "orders"}}}
	rewrite.ListOffsetsRequestIn(prefix, req)
	assert.Equal(t, "tA.orders", req.Topics[0].Topic)

	resp := &kmsg.ListOffsetsResponse{Topics: []kmsg.ListOffsetsResponseTopic{{Topic: "tA.orders"}}}
	rewrite.ListOffsetsResponseOut(prefix, resp)
	assert.Equal(t, "orders", resp.Topics[0].Topic)
}
