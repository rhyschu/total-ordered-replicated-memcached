package rpcache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client is safe for concurrent use. Set/Delete callers must reuse the same
// ID after ambiguous failures, and must not retry beyond the receipt window.
type Client struct {
	Endpoints []string
	Token     string
	HTTP      *http.Client
}

func NewClient(endpoints []string, token string) *Client {
	return &Client{Endpoints: append([]string(nil), endpoints...), Token: token, HTTP: &http.Client{Timeout: 4 * time.Second, Transport: &http.Transport{MaxIdleConns: 64, MaxIdleConnsPerHost: 32, IdleConnTimeout: 30 * time.Second}}}
}
func (c *Client) Call(ctx context.Context, req Request) (Result, error) {
	req.Version = "2.0"
	payload, err := json.Marshal(req)
	if err != nil {
		return Result{}, err
	}
	var last error = fmt.Errorf("no RPC endpoints")
	for _, endpoint := range c.Endpoints {
		if ctx.Err() != nil {
			return Result{}, ctx.Err()
		}
		request, err := http.NewRequestWithContext(ctx, "POST", endpoint+"/rpc", bytes.NewReader(payload))
		if err != nil {
			return Result{}, err
		}
		request.Header.Set("Authorization", "Bearer "+c.Token)
		request.Header.Set("Content-Type", "application/json")
		response, err := c.HTTP.Do(request)
		if err != nil {
			last = err
			continue
		}
		var out Response
		err = json.NewDecoder(io.LimitReader(response.Body, 65536)).Decode(&out)
		response.Body.Close()
		if response.StatusCode != 200 {
			last = fmt.Errorf("HTTP %d", response.StatusCode)
			if response.StatusCode == 401 || response.StatusCode == 403 {
				return Result{}, last
			}
			continue
		}
		if err != nil {
			return Result{}, err
		}
		if out.Version != "2.0" || out.ID != req.ID {
			return Result{}, fmt.Errorf("RPC response ID/version mismatch")
		}
		if out.Error != nil {
			last = fmt.Errorf("RPC %d: %s", out.Error.Code, out.Error.Message)
			if out.Error.Code == -32001 || out.Error.Code == -32000 {
				continue
			}
			return Result{}, last
		}
		if out.Result == nil {
			return Result{}, fmt.Errorf("missing RPC result")
		}
		return *out.Result, nil
	}
	return Result{}, last
}
