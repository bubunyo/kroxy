package proxy

import (
	"github.com/bubunyo/kroxy/protocol"
	"github.com/bubunyo/kroxy/rewrite"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (c *conn) handleCreateTopics(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrCreateTopicsRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleCreateTopics")
	}
	rewrite.CreateTopicsRequestIn(c.tenant.TopicPrefix, req)

	resp := kmsg.NewPtrCreateTopicsResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleCreateTopics")
	}
	rewrite.CreateTopicsResponseOut(c.tenant.TopicPrefix, resp)
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleDeleteTopics(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrDeleteTopicsRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleDeleteTopics")
	}
	rewrite.DeleteTopicsRequestIn(c.tenant.TopicPrefix, req)

	resp := kmsg.NewPtrDeleteTopicsResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleDeleteTopics")
	}
	rewrite.DeleteTopicsResponseOut(c.tenant.TopicPrefix, resp)
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleDescribeConfigs(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrDescribeConfigsRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleDescribeConfigs")
	}
	rewrite.DescribeConfigsRequestIn(c.tenant.TopicPrefix, req)

	resp := kmsg.NewPtrDescribeConfigsResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleDescribeConfigs")
	}
	rewrite.DescribeConfigsResponseOut(c.tenant.TopicPrefix, resp)
	return c.writeTypedResponse(hdr, resp)
}
