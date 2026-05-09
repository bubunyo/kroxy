package rewrite_test

import (
	"testing"

	"github.com/bubunyo/kroxy/rewrite"
	"github.com/stretchr/testify/assert"
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
