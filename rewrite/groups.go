package rewrite

import "github.com/twmb/franz-go/pkg/kmsg"

// CoordinatorTypeGroup / Transaction are the two `key_type` values the proxy
// recognises in FindCoordinator requests. Anything else is left alone (e.g.
// share-group coordinators introduced by KIP-932).
const (
	CoordinatorTypeGroup       int8 = 0
	CoordinatorTypeTransaction int8 = 1
)

// FindCoordinatorRequestIn rewrites the coordinator key(s) in a
// FindCoordinator request. For groups and transactions the key is the
// group ID or transactional.id and must be prefixed.
func FindCoordinatorRequestIn(prefix string, req *kmsg.FindCoordinatorRequest) {
	if req.CoordinatorType != CoordinatorTypeGroup && req.CoordinatorType != CoordinatorTypeTransaction {
		return
	}
	req.CoordinatorKey = PrefixIn(prefix, req.CoordinatorKey)
	for i, k := range req.CoordinatorKeys {
		req.CoordinatorKeys[i] = PrefixIn(prefix, k)
	}
}

// FindCoordinatorResponseOut rewrites the coordinator host/port to the
// proxy's advertised address (single virtual broker, node 0) and strips the
// tenant prefix from any returned coordinator key.
func FindCoordinatorResponseOut(prefix, advertisedHost string, advertisedPort int32, resp *kmsg.FindCoordinatorResponse) {
	resp.NodeID = 0
	resp.Host = advertisedHost
	resp.Port = advertisedPort
	for i := range resp.Coordinators {
		c := &resp.Coordinators[i]
		c.NodeID = 0
		c.Host = advertisedHost
		c.Port = advertisedPort
		c.Key = StripOut(prefix, c.Key)
	}
}

// OffsetCommitRequestIn prefixes the group ID and topic names in an
// OffsetCommit request.
func OffsetCommitRequestIn(prefix string, req *kmsg.OffsetCommitRequest) {
	req.Group = PrefixIn(prefix, req.Group)
	for i := range req.Topics {
		req.Topics[i].Topic = PrefixIn(prefix, req.Topics[i].Topic)
	}
}

// OffsetCommitResponseOut strips the tenant prefix from topic names in an
// OffsetCommit response.
func OffsetCommitResponseOut(prefix string, resp *kmsg.OffsetCommitResponse) {
	for i := range resp.Topics {
		resp.Topics[i].Topic = StripOut(prefix, resp.Topics[i].Topic)
	}
}

// OffsetFetchRequestIn prefixes the group ID(s) and topic names in an
// OffsetFetch request. v8+ uses the per-group form.
func OffsetFetchRequestIn(prefix string, req *kmsg.OffsetFetchRequest) {
	req.Group = PrefixIn(prefix, req.Group)
	for i := range req.Topics {
		req.Topics[i].Topic = PrefixIn(prefix, req.Topics[i].Topic)
	}
	for i := range req.Groups {
		g := &req.Groups[i]
		g.Group = PrefixIn(prefix, g.Group)
		for j := range g.Topics {
			g.Topics[j].Topic = PrefixIn(prefix, g.Topics[j].Topic)
		}
	}
}

// OffsetFetchResponseOut strips the tenant prefix from group IDs and topic
// names in an OffsetFetch response.
func OffsetFetchResponseOut(prefix string, resp *kmsg.OffsetFetchResponse) {
	for i := range resp.Topics {
		resp.Topics[i].Topic = StripOut(prefix, resp.Topics[i].Topic)
	}
	for i := range resp.Groups {
		g := &resp.Groups[i]
		g.Group = StripOut(prefix, g.Group)
		for j := range g.Topics {
			g.Topics[j].Topic = StripOut(prefix, g.Topics[j].Topic)
		}
	}
}

// OffsetDeleteRequestIn prefixes the group ID and topic names.
func OffsetDeleteRequestIn(prefix string, req *kmsg.OffsetDeleteRequest) {
	req.Group = PrefixIn(prefix, req.Group)
	for i := range req.Topics {
		req.Topics[i].Topic = PrefixIn(prefix, req.Topics[i].Topic)
	}
}

// OffsetDeleteResponseOut strips the tenant prefix from topic names.
func OffsetDeleteResponseOut(prefix string, resp *kmsg.OffsetDeleteResponse) {
	for i := range resp.Topics {
		resp.Topics[i].Topic = StripOut(prefix, resp.Topics[i].Topic)
	}
}

// JoinGroupRequestIn prefixes the group ID.
func JoinGroupRequestIn(prefix string, req *kmsg.JoinGroupRequest) {
	req.Group = PrefixIn(prefix, req.Group)
}

// SyncGroupRequestIn prefixes the group ID.
func SyncGroupRequestIn(prefix string, req *kmsg.SyncGroupRequest) {
	req.Group = PrefixIn(prefix, req.Group)
}

// HeartbeatRequestIn prefixes the group ID.
func HeartbeatRequestIn(prefix string, req *kmsg.HeartbeatRequest) {
	req.Group = PrefixIn(prefix, req.Group)
}

// LeaveGroupRequestIn prefixes the group ID.
func LeaveGroupRequestIn(prefix string, req *kmsg.LeaveGroupRequest) {
	req.Group = PrefixIn(prefix, req.Group)
}

// DescribeGroupsRequestIn prefixes every requested group ID.
func DescribeGroupsRequestIn(prefix string, req *kmsg.DescribeGroupsRequest) {
	for i, g := range req.Groups {
		req.Groups[i] = PrefixIn(prefix, g)
	}
}

// DescribeGroupsResponseOut strips the tenant prefix from each group ID in
// the response.
func DescribeGroupsResponseOut(prefix string, resp *kmsg.DescribeGroupsResponse) {
	for i := range resp.Groups {
		resp.Groups[i].Group = StripOut(prefix, resp.Groups[i].Group)
	}
}

// ListGroupsResponseOut filters the group list down to those owned by the
// tenant and strips the tenant prefix from the survivors.
func ListGroupsResponseOut(prefix string, resp *kmsg.ListGroupsResponse) {
	out := resp.Groups[:0]
	for _, g := range resp.Groups {
		if !BelongsToTenant(prefix, g.Group) {
			continue
		}
		g.Group = StripOut(prefix, g.Group)
		out = append(out, g)
	}
	resp.Groups = out
}

// DeleteGroupsRequestIn prefixes every group ID to be deleted.
func DeleteGroupsRequestIn(prefix string, req *kmsg.DeleteGroupsRequest) {
	for i, g := range req.Groups {
		req.Groups[i] = PrefixIn(prefix, g)
	}
}

// DeleteGroupsResponseOut strips the tenant prefix from each group ID in
// the response.
func DeleteGroupsResponseOut(prefix string, resp *kmsg.DeleteGroupsResponse) {
	for i := range resp.Groups {
		resp.Groups[i].Group = StripOut(prefix, resp.Groups[i].Group)
	}
}

// InitProducerIDRequestIn prefixes the transactional.id.
func InitProducerIDRequestIn(prefix string, req *kmsg.InitProducerIDRequest) {
	req.TransactionalID = PrefixInPtr(prefix, req.TransactionalID)
}

// AddPartitionsToTxnRequestIn prefixes the transactional.id and topic names.
// v4+ moves to a per-transaction batch form which is also handled.
func AddPartitionsToTxnRequestIn(prefix string, req *kmsg.AddPartitionsToTxnRequest) {
	req.TransactionalID = PrefixIn(prefix, req.TransactionalID)
	for i := range req.Topics {
		req.Topics[i].Topic = PrefixIn(prefix, req.Topics[i].Topic)
	}
	for i := range req.Transactions {
		tx := &req.Transactions[i]
		tx.TransactionalID = PrefixIn(prefix, tx.TransactionalID)
		for j := range tx.Topics {
			tx.Topics[j].Topic = PrefixIn(prefix, tx.Topics[j].Topic)
		}
	}
}

// AddPartitionsToTxnResponseOut strips the tenant prefix from topic names
// and transactional ids in the response.
func AddPartitionsToTxnResponseOut(prefix string, resp *kmsg.AddPartitionsToTxnResponse) {
	for i := range resp.Topics {
		resp.Topics[i].Topic = StripOut(prefix, resp.Topics[i].Topic)
	}
	for i := range resp.Transactions {
		tx := &resp.Transactions[i]
		tx.TransactionalID = StripOut(prefix, tx.TransactionalID)
		for j := range tx.Topics {
			tx.Topics[j].Topic = StripOut(prefix, tx.Topics[j].Topic)
		}
	}
}

// AddOffsetsToTxnRequestIn prefixes both the transactional.id and the
// group id.
func AddOffsetsToTxnRequestIn(prefix string, req *kmsg.AddOffsetsToTxnRequest) {
	req.TransactionalID = PrefixIn(prefix, req.TransactionalID)
	req.Group = PrefixIn(prefix, req.Group)
}

// EndTxnRequestIn prefixes the transactional.id.
func EndTxnRequestIn(prefix string, req *kmsg.EndTxnRequest) {
	req.TransactionalID = PrefixIn(prefix, req.TransactionalID)
}

// TxnOffsetCommitRequestIn prefixes the transactional.id, group id and
// topic names.
func TxnOffsetCommitRequestIn(prefix string, req *kmsg.TxnOffsetCommitRequest) {
	req.TransactionalID = PrefixIn(prefix, req.TransactionalID)
	req.Group = PrefixIn(prefix, req.Group)
	for i := range req.Topics {
		req.Topics[i].Topic = PrefixIn(prefix, req.Topics[i].Topic)
	}
}

// TxnOffsetCommitResponseOut strips the tenant prefix from topic names.
func TxnOffsetCommitResponseOut(prefix string, resp *kmsg.TxnOffsetCommitResponse) {
	for i := range resp.Topics {
		resp.Topics[i].Topic = StripOut(prefix, resp.Topics[i].Topic)
	}
}
