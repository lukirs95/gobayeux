package gobayeux_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sigmavirus24/gobayeux/v2"
	"github.com/sigmavirus24/gobayeux/v2/gobayeuxtest"
)

func TestNewClient(t *testing.T) {
	testCases := []struct {
		name          string
		serverAddress string
		shouldErr     bool
	}{
		{"valid url for server address", "https://example.com", false},
		{"invalid url for server address", "http://192.168.0.%31/", true},
	}

	for _, testCase := range testCases {
		tc := testCase
		t.Run(tc.name, func(t *testing.T) {
			_, err := gobayeux.NewClient(tc.serverAddress)
			if err != nil && !tc.shouldErr {
				t.Errorf("expected NewClient() to not return an err but it did, %q", err)
			} else if tc.shouldErr && err == nil {
				t.Error("expected NewClient() to err but it didn't")
			}
		})
	}
}

func TestSubscribe(t *testing.T) {
	client, err := gobayeux.NewClient("https://example.com", nil)
	if err != nil {
		t.Fatalf("expected a working client but got an err %q", err)
	}
	client.Subscribe("/foo/bar", nil)
}

func TestCanDoubleSubscribe(t *testing.T) {
	server := gobayeuxtest.NewServer(t)

	client, err := gobayeux.NewClient(
		"https://example.com",
		gobayeux.WithHTTPTransport(server),
		gobayeux.WithContinueOnError(true),
	)

	if err != nil {
		t.Fatalf("failed to create client (%v)", err)
	}

	done := make(chan error)
	msgs := make(chan []gobayeux.Message)
	errs := client.Start(context.Background())

	go func(msgs <-chan []gobayeux.Message, errs <-chan error, done chan<- error) {
		defer close(done)

		count := 0
		for count < 100 {
			select {
			case ms := <-msgs:
				t.Logf("count: %v", count)
				count += len(ms)
			case err := <-errs:
				if !strings.Contains(err.Error(), "already subscribed") {
					done <- err
					return
				}
			}
		}
	}(msgs, errs, done)

	client.Subscribe("/foo/bar", msgs)
	client.Subscribe("/foo/bar", msgs)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error from client (%v)", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("test timed out")
	}
}
