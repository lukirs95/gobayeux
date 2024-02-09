package gobayeux

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type BayeuxTransportWebsocket struct {
	conn          *websocket.Conn
	serverAddress *url.URL
	msgBuffer     chan []byte
	openRequest   *atomic.Uint32
	ready         *atomic.Bool
}

func NewBayeuxTransportWebsocket(serverAddress string) (*BayeuxTransportWebsocket, error) {
	parsedAddress, err := url.Parse(serverAddress)
	if err != nil {
		return nil, err
	}

	ready := &atomic.Bool{}
	ready.Store(false)

	openRequest := &atomic.Uint32{}
	openRequest.Store(0)

	return &BayeuxTransportWebsocket{
		conn:          nil,
		serverAddress: parsedAddress,
		msgBuffer:     make(chan []byte, 100),
		openRequest:   openRequest,
		ready:         ready,
	}, nil
}

// request sends data to the server and blocks until it received something
// as the bayeux protocol ensures, it's only sending data on request we
// have no problem here
func (t *BayeuxTransportWebsocket) request(ctx context.Context, msg []Message) ([]Message, error) {
	if !t.ready.Load() {
		return nil, fmt.Errorf("websocket not ready")
	}

	err := t.conn.WriteJSON(msg)
	if err != nil {
		return nil, err
	}
	t.openRequest.Add(1)

	raw := <-t.msgBuffer

	messages := make([]Message, 0)
	if err := json.Unmarshal(raw, &messages); err != nil {
		return nil, err
	}
	return messages, err
}

func (t *BayeuxTransportWebsocket) transportType() string {
	return ConnectionTypeWebsocket
}

// listen calls readLoop and in case of connection loss calls it again
// To stop the reconnection loop, cancel the context.
func (t *BayeuxTransportWebsocket) listen(ctx context.Context, reconnectAfter time.Duration, errChan chan error) {
	for {
		err := t.readLoop(ctx, errChan)
		if err != nil {
			errChan <- err
			<-time.After(reconnectAfter)
			continue
		}
		return
	}
}

// readLoop opens the websocket connection and reads from the
// connection in a blocking loop. The received data gets
// buffered in the msgBuffer channel. The implementation MUST
// read from the error channel, otherwise we don't read data
func (t *BayeuxTransportWebsocket) readLoop(ctx context.Context, errChan chan error) wsErrorI {
	defer t.ready.Store(false)

	timeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conn, _, err := websocket.DefaultDialer.DialContext(timeout, t.serverAddress.String(), nil)
	if err != nil {
		return wsErrorBad(err)
	}

	t.conn = conn

	t.ready.Store(true)

	for {
		if ctx.Err() != nil {
			return nil
		}

		messageType, raw, err := t.conn.ReadMessage()
		if err != nil {
			return wsErrorBad(err)
		}
		if messageType != websocket.TextMessage {
			errChan <- wsErrorUnsupported()
			continue
		}

		// ensures, that only requested data gets published.
		// some shitty implementations send an handshake ack
		// before the client even requested the handshake
		openRequest := t.openRequest.Load()
		if openRequest > 0 {
			t.msgBuffer <- raw
			t.openRequest.Store(openRequest - 1)
		} else {
			errChan <- wsErrorUndelivered()
		}
	}
}

type wsErrorI interface {
	error
}

type wsError string

func (e wsError) Error() string {
	return string(e)
}

func wsErrorUnsupported() wsError {
	return wsError("unsupported message type")
}

func wsErrorBad(err error) wsError {
	return wsError(err.Error())
}

func wsErrorUndelivered() wsError {
	return wsError("received unrequested message")
}
