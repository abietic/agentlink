package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"agentlink/internal/proto"
)

var ErrUnauthorized = errors.New("the server rejected this device's token")

// Client talks to agentlink-server.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

func NewClient(base string) *Client {
	return &Client{BaseURL: strings.TrimRight(base, "/"), HTTP: &http.Client{Timeout: 90 * time.Second}}
}

func (c *Client) Register(ctx context.Context, enrollToken, name string) (proto.RegisterResponse, error) {
	var out proto.RegisterResponse
	_, err := c.do(ctx, http.MethodPost, "/api/v1/register", enrollToken, proto.RegisterRequest{Name: name}, &out)
	return out, err
}

// Poll long-polls for the next task; nil means none arrived within the server's wait.
func (c *Client) Poll(ctx context.Context) (*proto.Task, error) {
	var t proto.Task
	code, err := c.do(ctx, http.MethodGet, "/api/v1/poll", c.Token, nil, &t)
	if err != nil || code == http.StatusNoContent {
		return nil, err
	}
	return &t, nil
}

func (c *Client) Result(ctx context.Context, taskID, text string) error {
	_, err := c.do(ctx, http.MethodPost, "/api/v1/tasks/"+url.PathEscape(taskID)+"/result", c.Token, proto.Result{Text: text}, nil)
	return err
}

func (c *Client) do(ctx context.Context, method, path, token string, in, out any) (int, error) {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusUnauthorized:
		return resp.StatusCode, ErrUnauthorized
	case resp.StatusCode/100 != 2:
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return resp.StatusCode, fmt.Errorf("%s %s: %s %s", method, path, resp.Status, bytes.TrimSpace(msg))
	case out != nil && resp.StatusCode != http.StatusNoContent:
		return resp.StatusCode, json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode, nil
}
