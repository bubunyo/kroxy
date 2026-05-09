package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/bubunyo/kroxy/resolver"
)

// Client is a minimal JSON-RPC 2.0 HTTP client for the admin service.
type Client struct {
	endpoint string
	http     *http.Client
	id       atomic.Uint64
}

// NewClient builds a Client that talks to the supplied endpoint URL (e.g.
// http://127.0.0.1:9095/rpc).
func NewClient(endpoint string) *Client {
	return &Client{
		endpoint: endpoint,
		http:     &http.Client{Timeout: 15 * time.Second},
	}
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      uint64 `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string { return e.Message }

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// Set creates a new tenant.
func (c *Client) Set(ctx context.Context, p SetParams) error {
	var out OKResult
	return c.call(ctx, "Tenants.Set", p, &out)
}

// Delete removes a tenant.
func (c *Client) Delete(ctx context.Context, username string) error {
	var out OKResult
	return c.call(ctx, "Tenants.Delete", DeleteParams{Username: username}, &out)
}

// List returns a snapshot of all tenants.
func (c *Client) List(ctx context.Context) (ListResult, error) {
	var out ListResult
	if err := c.call(ctx, "Tenants.List", struct{}{}, &out); err != nil {
		return ListResult{}, err
	}
	return out, nil
}

func (c *Client) call(ctx context.Context, method string, params, out any) error {
	body, err := json.Marshal(rpcRequest{
		JSONRPC: "2.0",
		ID:      c.id.Add(1),
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return errors.Wrap(err, "Client.call")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return errors.Wrap(err, "Client.call")
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return errors.Wrap(err, "Client.call")
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return errors.Errorf("Client.call: HTTP %d: %s", resp.StatusCode, string(raw))
	}

	var envelope rpcResponse
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return errors.Wrap(err, "Client.call: decode")
	}
	if envelope.Error != nil {
		return mapClientErr(envelope.Error)
	}
	if out != nil && len(envelope.Result) > 0 {
		if err := json.Unmarshal(envelope.Result, out); err != nil {
			return errors.Wrap(err, "Client.call: decode result")
		}
	}
	return nil
}

func mapClientErr(e *rpcError) error {
	switch e.Code {
	case CodeDuplicate:
		return errors.Wrapf(resolver.ErrDuplicate, "admin: %s", e.Message)
	case CodeNotFound:
		return errors.Wrapf(resolver.ErrNotFound, "admin: %s", e.Message)
	case CodeInvalid:
		return errors.Wrapf(resolver.ErrInvalidUser, "admin: %s", e.Message)
	default:
		return errors.Errorf("admin: rpc error %d: %s", e.Code, e.Message)
	}
}
