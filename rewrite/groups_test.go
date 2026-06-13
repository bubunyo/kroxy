package rewrite_test

import (
	"testing"

	"github.com/bubunyo/kroxy/rewrite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const tp = "tA."

func TestFindCoordinator_RoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		ctype     int8
		key       string
		keys      []string
		wantKey   string
		wantKeys  []string
		wantHost  string
		wantNode  int32
		stripBack string
	}{
		{
			name:    "group single",
			ctype:   rewrite.CoordinatorTypeGroup,
			key:     "consumer-1",
			wantKey: "tA.consumer-1",
		},
		{
			name:     "txn batch",
			ctype:    rewrite.CoordinatorTypeTransaction,
			keys:     []string{"txn-1", "txn-2"},
			wantKeys: []string{"tA.txn-1", "tA.txn-2"},
		},
		{
			name:    "share group key not rewritten",
			ctype:   2,
			key:     "share-1",
			wantKey: "share-1",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := &kmsg.FindCoordinatorRequest{
				CoordinatorType: tt.ctype,
				CoordinatorKey:  tt.key,
				CoordinatorKeys: append([]string(nil), tt.keys...),
			}
			rewrite.FindCoordinatorRequestIn(tp, req)
			if tt.wantKey != "" {
				assert.Equal(t, tt.wantKey, req.CoordinatorKey)
			}
			if tt.wantKeys != nil {
				assert.Equal(t, tt.wantKeys, req.CoordinatorKeys)
			}

			resp := &kmsg.FindCoordinatorResponse{
				NodeID: 11, Host: "real-broker", Port: 9092,
				Coordinators: []kmsg.FindCoordinatorResponseCoordinator{
					{Key: "tA.consumer-1", NodeID: 11, Host: "real-broker", Port: 9092},
				},
			}
			rewrite.FindCoordinatorResponseOut(tp, "kroxy", 9092, resp)
			assert.Equal(t, int32(0), resp.NodeID)
			assert.Equal(t, "kroxy", resp.Host)
			assert.Equal(t, "consumer-1", resp.Coordinators[0].Key)
			assert.Equal(t, int32(0), resp.Coordinators[0].NodeID)
		})
	}
}

func TestOffsetCommit_RoundTrip(t *testing.T) {
	t.Parallel()
	req := &kmsg.OffsetCommitRequest{
		Group:  "consumer-1",
		Topics: []kmsg.OffsetCommitRequestTopic{{Topic: "orders"}},
	}
	rewrite.OffsetCommitRequestIn(tp, req)
	assert.Equal(t, "tA.consumer-1", req.Group)
	assert.Equal(t, "tA.orders", req.Topics[0].Topic)

	resp := &kmsg.OffsetCommitResponse{
		Topics: []kmsg.OffsetCommitResponseTopic{{Topic: "tA.orders"}},
	}
	rewrite.OffsetCommitResponseOut(tp, resp)
	assert.Equal(t, "orders", resp.Topics[0].Topic)
}

func TestOffsetFetch_RoundTrip_BothShapes(t *testing.T) {
	t.Parallel()
	req := &kmsg.OffsetFetchRequest{
		Group:  "consumer-1",
		Topics: []kmsg.OffsetFetchRequestTopic{{Topic: "orders"}},
		Groups: []kmsg.OffsetFetchRequestGroup{{
			Group:  "consumer-2",
			Topics: []kmsg.OffsetFetchRequestGroupTopic{{Topic: "payments"}},
		}},
	}
	rewrite.OffsetFetchRequestIn(tp, req)
	assert.Equal(t, "tA.consumer-1", req.Group)
	assert.Equal(t, "tA.orders", req.Topics[0].Topic)
	assert.Equal(t, "tA.consumer-2", req.Groups[0].Group)
	assert.Equal(t, "tA.payments", req.Groups[0].Topics[0].Topic)

	resp := &kmsg.OffsetFetchResponse{
		Topics: []kmsg.OffsetFetchResponseTopic{{Topic: "tA.orders"}},
		Groups: []kmsg.OffsetFetchResponseGroup{{
			Group:  "tA.consumer-2",
			Topics: []kmsg.OffsetFetchResponseGroupTopic{{Topic: "tA.payments"}},
		}},
	}
	rewrite.OffsetFetchResponseOut(tp, resp)
	assert.Equal(t, "orders", resp.Topics[0].Topic)
	assert.Equal(t, "consumer-2", resp.Groups[0].Group)
	assert.Equal(t, "payments", resp.Groups[0].Topics[0].Topic)
}

func TestGroupLifecycle_PrefixesGroupID(t *testing.T) {
	t.Parallel()

	jg := &kmsg.JoinGroupRequest{Group: "consumer-1"}
	rewrite.JoinGroupRequestIn(tp, jg)
	assert.Equal(t, "tA.consumer-1", jg.Group)

	sg := &kmsg.SyncGroupRequest{Group: "consumer-1"}
	rewrite.SyncGroupRequestIn(tp, sg)
	assert.Equal(t, "tA.consumer-1", sg.Group)

	hb := &kmsg.HeartbeatRequest{Group: "consumer-1"}
	rewrite.HeartbeatRequestIn(tp, hb)
	assert.Equal(t, "tA.consumer-1", hb.Group)

	lg := &kmsg.LeaveGroupRequest{Group: "consumer-1"}
	rewrite.LeaveGroupRequestIn(tp, lg)
	assert.Equal(t, "tA.consumer-1", lg.Group)
}

func TestDescribeGroups_RoundTrip(t *testing.T) {
	t.Parallel()
	req := &kmsg.DescribeGroupsRequest{Groups: []string{"a", "b"}}
	rewrite.DescribeGroupsRequestIn(tp, req)
	assert.Equal(t, []string{"tA.a", "tA.b"}, req.Groups)

	resp := &kmsg.DescribeGroupsResponse{
		Groups: []kmsg.DescribeGroupsResponseGroup{{Group: "tA.a"}, {Group: "tA.b"}},
	}
	rewrite.DescribeGroupsResponseOut(tp, resp)
	assert.Equal(t, "a", resp.Groups[0].Group)
	assert.Equal(t, "b", resp.Groups[1].Group)
}

func TestListGroups_FiltersOtherTenants(t *testing.T) {
	t.Parallel()
	resp := &kmsg.ListGroupsResponse{
		Groups: []kmsg.ListGroupsResponseGroup{
			{Group: "tA.consumer-1"},
			{Group: "tB.consumer-1"},
			{Group: "tA.consumer-2"},
		},
	}
	rewrite.ListGroupsResponseOut(tp, resp)
	assert.Len(t, resp.Groups, 2)
	assert.Equal(t, "consumer-1", resp.Groups[0].Group)
	assert.Equal(t, "consumer-2", resp.Groups[1].Group)
}

func TestDeleteGroups_RoundTrip(t *testing.T) {
	t.Parallel()
	req := &kmsg.DeleteGroupsRequest{Groups: []string{"a", "b"}}
	rewrite.DeleteGroupsRequestIn(tp, req)
	assert.Equal(t, []string{"tA.a", "tA.b"}, req.Groups)

	resp := &kmsg.DeleteGroupsResponse{
		Groups: []kmsg.DeleteGroupsResponseGroup{{Group: "tA.a"}, {Group: "tA.b"}},
	}
	rewrite.DeleteGroupsResponseOut(tp, resp)
	assert.Equal(t, "a", resp.Groups[0].Group)
	assert.Equal(t, "b", resp.Groups[1].Group)
}

func TestTransactions_PrefixesTxnAndGroup(t *testing.T) {
	t.Parallel()

	ip := &kmsg.InitProducerIDRequest{TransactionalID: strPtr("txn-1")}
	rewrite.InitProducerIDRequestIn(tp, ip)
	assert.Equal(t, "tA.txn-1", *ip.TransactionalID)

	ap := &kmsg.AddPartitionsToTxnRequest{
		TransactionalID: "txn-1",
		Topics:          []kmsg.AddPartitionsToTxnRequestTopic{{Topic: "orders"}},
		Transactions: []kmsg.AddPartitionsToTxnRequestTransaction{{
			TransactionalID: "txn-2",
			Topics: []kmsg.AddPartitionsToTxnRequestTransactionTopic{
				{Topic: "payments"},
			},
		}},
	}
	rewrite.AddPartitionsToTxnRequestIn(tp, ap)
	assert.Equal(t, "tA.txn-1", ap.TransactionalID)
	assert.Equal(t, "tA.orders", ap.Topics[0].Topic)
	assert.Equal(t, "tA.txn-2", ap.Transactions[0].TransactionalID)
	assert.Equal(t, "tA.payments", ap.Transactions[0].Topics[0].Topic)

	apr := &kmsg.AddPartitionsToTxnResponse{
		Topics: []kmsg.AddPartitionsToTxnResponseTopic{{Topic: "tA.orders"}},
		Transactions: []kmsg.AddPartitionsToTxnResponseTransaction{{
			TransactionalID: "tA.txn-2",
			Topics: []kmsg.AddPartitionsToTxnResponseTransactionTopic{
				{Topic: "tA.payments"},
			},
		}},
	}
	rewrite.AddPartitionsToTxnResponseOut(tp, apr)
	assert.Equal(t, "orders", apr.Topics[0].Topic)
	assert.Equal(t, "txn-2", apr.Transactions[0].TransactionalID)
	assert.Equal(t, "payments", apr.Transactions[0].Topics[0].Topic)

	ao := &kmsg.AddOffsetsToTxnRequest{TransactionalID: "txn-1", Group: "consumer-1"}
	rewrite.AddOffsetsToTxnRequestIn(tp, ao)
	assert.Equal(t, "tA.txn-1", ao.TransactionalID)
	assert.Equal(t, "tA.consumer-1", ao.Group)

	end := &kmsg.EndTxnRequest{TransactionalID: "txn-1"}
	rewrite.EndTxnRequestIn(tp, end)
	assert.Equal(t, "tA.txn-1", end.TransactionalID)

	tc := &kmsg.TxnOffsetCommitRequest{
		TransactionalID: "txn-1", Group: "consumer-1",
		Topics: []kmsg.TxnOffsetCommitRequestTopic{{Topic: "orders"}},
	}
	rewrite.TxnOffsetCommitRequestIn(tp, tc)
	assert.Equal(t, "tA.txn-1", tc.TransactionalID)
	assert.Equal(t, "tA.consumer-1", tc.Group)
	assert.Equal(t, "tA.orders", tc.Topics[0].Topic)

	tcr := &kmsg.TxnOffsetCommitResponse{
		Topics: []kmsg.TxnOffsetCommitResponseTopic{{Topic: "tA.orders"}},
	}
	rewrite.TxnOffsetCommitResponseOut(tp, tcr)
	assert.Equal(t, "orders", tcr.Topics[0].Topic)
}

func encSub(topics []string, owned map[string][]int32) []byte {
	m := kmsg.NewConsumerMemberMetadata()
	m.Version = 1
	m.Topics = topics
	m.UserData = []byte("ud")
	for tname, parts := range owned {
		op := kmsg.NewConsumerMemberMetadataOwnedPartition()
		op.Topic = tname
		op.Partitions = parts
		m.OwnedPartitions = append(m.OwnedPartitions, op)
	}
	return m.AppendTo(nil)
}

func decSub(t *testing.T, b []byte) kmsg.ConsumerMemberMetadata {
	t.Helper()
	var m kmsg.ConsumerMemberMetadata
	require.NoError(t, m.ReadFrom(b))
	return m
}

func encAsg(topic string, parts []int32) []byte {
	a := kmsg.NewConsumerMemberAssignment()
	a.Version = 1
	at := kmsg.NewConsumerMemberAssignmentTopic()
	at.Topic = topic
	at.Partitions = parts
	a.Topics = append(a.Topics, at)
	a.UserData = []byte("ud")
	return a.AppendTo(nil)
}

func decAsg(t *testing.T, b []byte) kmsg.ConsumerMemberAssignment {
	t.Helper()
	var a kmsg.ConsumerMemberAssignment
	require.NoError(t, a.ReadFrom(b))
	return a
}

func TestJoinGroup_RewritesSubscriptionTopics(t *testing.T) {
	t.Parallel()

	req := &kmsg.JoinGroupRequest{
		Group:        "consumer-1",
		ProtocolType: "consumer",
		Protocols: []kmsg.JoinGroupRequestProtocol{
			{Name: "range", Metadata: encSub([]string{"orders", "events"}, map[string][]int32{"orders": {0, 1}})},
		},
	}
	rewrite.JoinGroupRequestIn(tp, req)
	assert.Equal(t, "tA.consumer-1", req.Group)

	got := decSub(t, req.Protocols[0].Metadata)
	assert.Equal(t, []string{"tA.orders", "tA.events"}, got.Topics)
	require.Len(t, got.OwnedPartitions, 1)
	assert.Equal(t, "tA.orders", got.OwnedPartitions[0].Topic)
	assert.Equal(t, []int32{0, 1}, got.OwnedPartitions[0].Partitions)
	assert.Equal(t, []byte("ud"), got.UserData)

	pt := "consumer"
	resp := &kmsg.JoinGroupResponse{
		ProtocolType: &pt,
		Members: []kmsg.JoinGroupResponseMember{
			{MemberID: "m1", ProtocolMetadata: encSub([]string{"tA.orders", "tA.events"}, nil)},
		},
	}
	rewrite.JoinGroupResponseOut(tp, resp)
	gotResp := decSub(t, resp.Members[0].ProtocolMetadata)
	assert.Equal(t, []string{"orders", "events"}, gotResp.Topics)
}

func TestSyncGroup_RewritesAssignmentTopics(t *testing.T) {
	t.Parallel()

	req := &kmsg.SyncGroupRequest{
		Group: "consumer-1",
		GroupAssignment: []kmsg.SyncGroupRequestGroupAssignment{
			{MemberID: "m1", MemberAssignment: encAsg("orders", []int32{0, 1})},
		},
	}
	rewrite.SyncGroupRequestIn(tp, req)
	assert.Equal(t, "tA.consumer-1", req.Group)
	gotReq := decAsg(t, req.GroupAssignment[0].MemberAssignment)
	require.Len(t, gotReq.Topics, 1)
	assert.Equal(t, "tA.orders", gotReq.Topics[0].Topic)
	assert.Equal(t, []int32{0, 1}, gotReq.Topics[0].Partitions)

	resp := &kmsg.SyncGroupResponse{MemberAssignment: encAsg("tA.orders", []int32{0, 1})}
	rewrite.SyncGroupResponseOut(tp, resp)
	gotResp := decAsg(t, resp.MemberAssignment)
	assert.Equal(t, "orders", gotResp.Topics[0].Topic)
	assert.Equal(t, []byte("ud"), gotResp.UserData)
}

func TestJoinGroup_NonConsumerProtocolUntouched(t *testing.T) {
	t.Parallel()

	blob := []byte("opaque-connect-payload")
	req := &kmsg.JoinGroupRequest{
		Group:        "connect-cluster",
		ProtocolType: "connect",
		Protocols:    []kmsg.JoinGroupRequestProtocol{{Name: "default", Metadata: blob}},
	}
	rewrite.JoinGroupRequestIn(tp, req)
	assert.Equal(t, "tA.connect-cluster", req.Group)
	assert.Equal(t, blob, req.Protocols[0].Metadata, "non-consumer protocol metadata must pass through unchanged")
}

func TestJoinGroup_EmptyPrefixLeavesBlobsByteIdentical(t *testing.T) {
	t.Parallel()

	orig := encSub([]string{"orders"}, nil)
	cp := append([]byte(nil), orig...)
	req := &kmsg.JoinGroupRequest{
		Group:        "consumer-1",
		ProtocolType: "consumer",
		Protocols:    []kmsg.JoinGroupRequestProtocol{{Name: "range", Metadata: cp}},
	}
	rewrite.JoinGroupRequestIn("", req)
	assert.Equal(t, orig, req.Protocols[0].Metadata)
}
