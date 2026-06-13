package proxy

import (
	"log/slog"
	"net"
	"strconv"

	"github.com/bubunyo/kroxy/protocol"
	"github.com/bubunyo/kroxy/rewrite"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// advertised splits the configured advertised address into host + port for
// Metadata broker rewriting.
func (c *conn) advertised() (string, int32, error) {
	host, portStr, err := net.SplitHostPort(c.cfg.Advertised)
	if err != nil {
		return "", 0, errors.Wrap(err, "advertised")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, errors.Wrap(err, "advertised")
	}
	return host, int32(port), nil
}

// roundTripTyped sends a typed kmsg.Request to the upstream and decodes the
// response into resp. It is the workhorse for every rewrite handler.
func (c *conn) roundTripTyped(req kmsg.Request, resp kmsg.Response, clientID string) error {
	body, err := c.upstream.RoundTripRequest(req, clientID)
	if err != nil {
		return errors.Wrap(err, "roundTripTyped")
	}
	resp.SetVersion(req.GetVersion())
	if err := resp.ReadFrom(body); err != nil {
		return errors.Wrap(err, "roundTripTyped")
	}
	return nil
}

// writeTypedResponse encodes resp with the original client correlation ID +
// header version and writes the resulting frame back to the client.
func (c *conn) writeTypedResponse(hdr protocol.RequestHeader, resp kmsg.Response) error {
	frame := protocol.EncodeResponse(resp, hdr.CorrelationID, hdr.APIKey, hdr.APIVersion)
	return protocol.WriteFrame(c.nc, frame)
}

func (c *conn) handleMetadata(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrMetadataRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleMetadata")
	}
	rewrite.MetadataRequestIn(c.tenant.TopicPrefix, req)
	if c.log.Enabled(c.ctx, slog.LevelDebug) {
		c.log.DebugContext(c.ctx, "metadata -> broker", "v", hdr.APIVersion, "tenant", c.tenant.ID, "req", dbgMetadataReq(req))
	}

	resp := kmsg.NewPtrMetadataResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleMetadata")
	}
	if c.log.Enabled(c.ctx, slog.LevelDebug) {
		c.log.DebugContext(c.ctx, "metadata <- broker", "resp", dbgMetadataResp(resp))
	}

	host, port, err := c.advertised()
	if err != nil {
		return errors.Wrap(err, "handleMetadata")
	}
	rewrite.MetadataResponseOut(c.tenant.TopicPrefix, host, port, resp)
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleProduce(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrProduceRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleProduce")
	}
	rewrite.ProduceRequestIn(c.tenant.TopicPrefix, req)

	// Produce with Acks=0 has no upstream response, which breaks our
	// synchronous request/response loop. v1 rejects it explicitly; clients
	// can retry with acks=1 or acks=all.
	if req.Acks == 0 {
		return errors.New("handleProduce: acks=0 produce is not supported by the proxy")
	}

	resp := kmsg.NewPtrProduceResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleProduce")
	}
	rewrite.ProduceResponseOut(c.tenant.TopicPrefix, resp)
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleFetch(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrFetchRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleFetch")
	}
	rewrite.FetchRequestIn(c.tenant.TopicPrefix, req)
	if c.log.Enabled(c.ctx, slog.LevelDebug) {
		c.log.DebugContext(c.ctx, "fetch -> broker", "v", hdr.APIVersion, "tenant", c.tenant.ID, "req", dbgFetchReq(req))
	}

	resp := kmsg.NewPtrFetchResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleFetch")
	}
	if c.log.Enabled(c.ctx, slog.LevelDebug) {
		c.log.DebugContext(c.ctx, "fetch <- broker", "resp", dbgFetchResp(resp))
	}
	rewrite.FetchResponseOut(c.tenant.TopicPrefix, resp)
	return c.writeTypedResponse(hdr, resp)
}

func (c *conn) handleListOffsets(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrListOffsetsRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleListOffsets")
	}
	rewrite.ListOffsetsRequestIn(c.tenant.TopicPrefix, req)
	if c.log.Enabled(c.ctx, slog.LevelDebug) {
		c.log.DebugContext(c.ctx, "listoffsets -> broker", "v", hdr.APIVersion, "tenant", c.tenant.ID, "req", dbgListOffsetsReq(req))
	}

	resp := kmsg.NewPtrListOffsetsResponse()
	if err := c.roundTripTyped(req, resp, hdr.ClientID); err != nil {
		return errors.Wrap(err, "handleListOffsets")
	}
	if c.log.Enabled(c.ctx, slog.LevelDebug) {
		c.log.DebugContext(c.ctx, "listoffsets <- broker", "resp", dbgListOffsetsResp(resp))
	}
	rewrite.ListOffsetsResponseOut(c.tenant.TopicPrefix, resp)
	return c.writeTypedResponse(hdr, resp)
}
