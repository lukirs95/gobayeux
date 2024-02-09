package gobayeux

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"

	"golang.org/x/net/publicsuffix"
)

type BayeuxTransportHttp struct {
	client        *http.Client
	serverAddress *url.URL
}

func NewBayeuxTransportHttp(client *http.Client, transport http.RoundTripper, serverAddress string) (*BayeuxTransportHttp, error) {
	if client == nil {
		jar, err := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
		if err != nil {
			return nil, err
		}

		client = &http.Client{
			Transport:     http.DefaultClient.Transport,
			CheckRedirect: http.DefaultClient.CheckRedirect,
			Jar:           jar,
			Timeout:       http.DefaultClient.Timeout,
		}
	}
	if transport == nil {
		transport = http.DefaultTransport
	}
	client.Transport = transport

	parsedAddress, err := url.Parse(serverAddress)
	if err != nil {
		return nil, err
	}

	return &BayeuxTransportHttp{
		client:        client,
		serverAddress: parsedAddress,
	}, nil
}

func (t *BayeuxTransportHttp) request(ctx context.Context, ms []Message) ([]Message, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(ms); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", t.serverAddress.String(), &buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	response, err := t.client.Do(req)
	if err != nil {
		return nil, BadResponseError{response.StatusCode, response.Status, nil}
	}
	return t.parseResponse(response)
}

func (t *BayeuxTransportHttp) parseResponse(resp *http.Response) ([]Message, error) {
	messages := make([]Message, 0)
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, BadResponseError{resp.StatusCode, resp.Status, nil}
		}

		return nil, BadResponseError{resp.StatusCode, resp.Status, body}
	}

	if err := json.NewDecoder(resp.Body).Decode(&messages); err != nil {
		return nil, err
	}
	return messages, nil
}

func (t *BayeuxTransportHttp) transportType() string {
	return ConnectionTypeLongPolling
}
