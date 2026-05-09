package rewrite_test

import (
	"testing"

	"github.com/bubunyo/kroxy/rewrite"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestCreateTopics_RoundTrip(t *testing.T) {
	t.Parallel()

	req := &kmsg.CreateTopicsRequest{
		Topics: []kmsg.CreateTopicsRequestTopic{
			{Topic: "events"},
			{Topic: "audit"},
		},
	}
	rewrite.CreateTopicsRequestIn(tp, req)
	assert.Equal(t, []string{tp + "events", tp + "audit"},
		[]string{req.Topics[0].Topic, req.Topics[1].Topic})

	resp := &kmsg.CreateTopicsResponse{
		Topics: []kmsg.CreateTopicsResponseTopic{
			{Topic: tp + "events"},
			{Topic: tp + "audit"},
		},
	}
	rewrite.CreateTopicsResponseOut(tp, resp)
	assert.Equal(t, []string{"events", "audit"},
		[]string{resp.Topics[0].Topic, resp.Topics[1].Topic})
}

func TestDeleteTopics_RoundTrip_LegacyFlatList(t *testing.T) {
	t.Parallel()

	req := &kmsg.DeleteTopicsRequest{
		TopicNames: []string{"events", "audit"},
	}
	rewrite.DeleteTopicsRequestIn(tp, req)
	assert.Equal(t, []string{tp + "events", tp + "audit"}, req.TopicNames)

	resp := &kmsg.DeleteTopicsResponse{
		Topics: []kmsg.DeleteTopicsResponseTopic{
			{Topic: strPtr(tp + "events")},
			{Topic: strPtr(tp + "audit")},
		},
	}
	rewrite.DeleteTopicsResponseOut(tp, resp)
	assert.Equal(t, "events", *resp.Topics[0].Topic)
	assert.Equal(t, "audit", *resp.Topics[1].Topic)
}

func TestDeleteTopics_RoundTrip_StructForm(t *testing.T) {
	t.Parallel()

	req := &kmsg.DeleteTopicsRequest{
		Topics: []kmsg.DeleteTopicsRequestTopic{
			{Topic: strPtr("events")},
			{Topic: strPtr("audit")},
		},
	}
	rewrite.DeleteTopicsRequestIn(tp, req)
	assert.Equal(t, tp+"events", *req.Topics[0].Topic)
	assert.Equal(t, tp+"audit", *req.Topics[1].Topic)
}

func TestDescribeConfigs_RoundTrip(t *testing.T) {
	t.Parallel()

	req := &kmsg.DescribeConfigsRequest{
		Resources: []kmsg.DescribeConfigsRequestResource{
			{ResourceType: 2, ResourceName: "events"},  // TOPIC
			{ResourceType: 4, ResourceName: "broker0"}, // BROKER → untouched
			{ResourceType: 32, ResourceName: "g1"},     // GROUP_CONFIG
		},
	}
	rewrite.DescribeConfigsRequestIn(tp, req)
	assert.Equal(t, tp+"events", req.Resources[0].ResourceName)
	assert.Equal(t, "broker0", req.Resources[1].ResourceName)
	assert.Equal(t, tp+"g1", req.Resources[2].ResourceName)

	resp := &kmsg.DescribeConfigsResponse{
		Resources: []kmsg.DescribeConfigsResponseResource{
			{ResourceType: 2, ResourceName: tp + "events"},
			{ResourceType: 2, ResourceName: "tB.other"}, // foreign tenant → dropped
			{ResourceType: 4, ResourceName: "broker0"},  // broker → kept untouched
			{ResourceType: 32, ResourceName: tp + "g1"},
		},
	}
	rewrite.DescribeConfigsResponseOut(tp, resp)
	if assert.Len(t, resp.Resources, 3) {
		assert.Equal(t, "events", resp.Resources[0].ResourceName)
		assert.Equal(t, "broker0", resp.Resources[1].ResourceName)
		assert.Equal(t, "g1", resp.Resources[2].ResourceName)
	}
}
