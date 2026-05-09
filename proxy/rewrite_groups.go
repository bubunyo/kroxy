package proxy

import (
	"github.com/bubunyo/kroxy/protocol"
	"github.com/bubunyo/kroxy/rewrite"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (c *conn) handleFindCoordinator(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrFindCoordinatorRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleFindCoordinator")
	}
	rewrite.FindCoordinatorRequestIn(c.tenant.TopicPrefix, req)

	resp := kmsg.NewPtrFindCoordinatorResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleFindCoordinator")
	}

	host, port, err := c.advertised()
	if err != nil {
		return errors.Wrap(err, "handleFindCoordinator")
	}
	rewrite.FindCoordinatorResponseOut(c.tenant.TopicPrefix, host, port, resp)
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleOffsetCommit(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrOffsetCommitRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleOffsetCommit")
	}
	rewrite.OffsetCommitRequestIn(c.tenant.TopicPrefix, req)

	resp := kmsg.NewPtrOffsetCommitResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleOffsetCommit")
	}
	rewrite.OffsetCommitResponseOut(c.tenant.TopicPrefix, resp)
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleOffsetFetch(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrOffsetFetchRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleOffsetFetch")
	}
	rewrite.OffsetFetchRequestIn(c.tenant.TopicPrefix, req)

	resp := kmsg.NewPtrOffsetFetchResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleOffsetFetch")
	}
	rewrite.OffsetFetchResponseOut(c.tenant.TopicPrefix, resp)
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleOffsetDelete(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrOffsetDeleteRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleOffsetDelete")
	}
	rewrite.OffsetDeleteRequestIn(c.tenant.TopicPrefix, req)

	resp := kmsg.NewPtrOffsetDeleteResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleOffsetDelete")
	}
	rewrite.OffsetDeleteResponseOut(c.tenant.TopicPrefix, resp)
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleJoinGroup(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrJoinGroupRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleJoinGroup")
	}
	rewrite.JoinGroupRequestIn(c.tenant.TopicPrefix, req)

	resp := kmsg.NewPtrJoinGroupResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleJoinGroup")
	}
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleSyncGroup(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrSyncGroupRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleSyncGroup")
	}
	rewrite.SyncGroupRequestIn(c.tenant.TopicPrefix, req)

	resp := kmsg.NewPtrSyncGroupResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleSyncGroup")
	}
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleHeartbeat(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrHeartbeatRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleHeartbeat")
	}
	rewrite.HeartbeatRequestIn(c.tenant.TopicPrefix, req)

	resp := kmsg.NewPtrHeartbeatResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleHeartbeat")
	}
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleLeaveGroup(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrLeaveGroupRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleLeaveGroup")
	}
	rewrite.LeaveGroupRequestIn(c.tenant.TopicPrefix, req)

	resp := kmsg.NewPtrLeaveGroupResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleLeaveGroup")
	}
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleDescribeGroups(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrDescribeGroupsRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleDescribeGroups")
	}
	rewrite.DescribeGroupsRequestIn(c.tenant.TopicPrefix, req)

	resp := kmsg.NewPtrDescribeGroupsResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleDescribeGroups")
	}
	rewrite.DescribeGroupsResponseOut(c.tenant.TopicPrefix, resp)
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleListGroups(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrListGroupsRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleListGroups")
	}
	// Request itself has no group fields to rewrite.
	resp := kmsg.NewPtrListGroupsResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleListGroups")
	}
	rewrite.ListGroupsResponseOut(c.tenant.TopicPrefix, resp)
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleDeleteGroups(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrDeleteGroupsRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleDeleteGroups")
	}
	rewrite.DeleteGroupsRequestIn(c.tenant.TopicPrefix, req)

	resp := kmsg.NewPtrDeleteGroupsResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleDeleteGroups")
	}
	rewrite.DeleteGroupsResponseOut(c.tenant.TopicPrefix, resp)
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleInitProducerID(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrInitProducerIDRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleInitProducerID")
	}
	rewrite.InitProducerIDRequestIn(c.tenant.TopicPrefix, req)

	resp := kmsg.NewPtrInitProducerIDResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleInitProducerID")
	}
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleAddPartitionsToTxn(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrAddPartitionsToTxnRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleAddPartitionsToTxn")
	}
	rewrite.AddPartitionsToTxnRequestIn(c.tenant.TopicPrefix, req)

	resp := kmsg.NewPtrAddPartitionsToTxnResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleAddPartitionsToTxn")
	}
	rewrite.AddPartitionsToTxnResponseOut(c.tenant.TopicPrefix, resp)
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleAddOffsetsToTxn(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrAddOffsetsToTxnRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleAddOffsetsToTxn")
	}
	rewrite.AddOffsetsToTxnRequestIn(c.tenant.TopicPrefix, req)

	resp := kmsg.NewPtrAddOffsetsToTxnResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleAddOffsetsToTxn")
	}
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleEndTxn(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrEndTxnRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleEndTxn")
	}
	rewrite.EndTxnRequestIn(c.tenant.TopicPrefix, req)

	resp := kmsg.NewPtrEndTxnResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleEndTxn")
	}
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleTxnOffsetCommit(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrTxnOffsetCommitRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleTxnOffsetCommit")
	}
	rewrite.TxnOffsetCommitRequestIn(c.tenant.TopicPrefix, req)

	resp := kmsg.NewPtrTxnOffsetCommitResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleTxnOffsetCommit")
	}
	rewrite.TxnOffsetCommitResponseOut(c.tenant.TopicPrefix, resp)
	return c.writeTypedResponse(hdr, resp)
}
