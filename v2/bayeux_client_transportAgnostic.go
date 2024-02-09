package gobayeux

import (
	"context"
	"fmt"
	"time"
)

type BayeuxClientT struct {
	stateMachine *ConnectionStateMachine
	client       transport
	state        *clientState
	exts         []MessageExtender
	logger       Logger
}

// NewBayeuxClient initializes a BayeuxClient for the user
func NewBayeuxClientT(client transport, logger Logger) (*BayeuxClientT, error) {
	if client == nil {
		return nil, fmt.Errorf("client must not be nil")
	}

	if logger == nil {
		logger = newNullLogger()
	}

	return &BayeuxClientT{
		stateMachine: NewConnectionStateMachine(),
		client:       client,
		state:        &clientState{},
		logger:       logger,
	}, nil
}

// Handshake sends the handshake request to the Bayeux Server
func (b *BayeuxClientT) Handshake(ctx context.Context) ([]Message, error) {
	logger := b.logger.WithField("at", "handshake")
	start := time.Now()
	logger.Debug("starting")
	if err := b.stateMachine.ProcessEvent(handshakeSent); err != nil {
		logger.WithError(err).Debug("invalid action for current state")
		return nil, HandshakeFailedError{err}
	}
	builder := NewHandshakeRequestBuilder()
	if err := builder.AddVersion("1.0"); err != nil {
		return nil, HandshakeFailedError{err}
	}
	if err := builder.AddSupportedConnectionType(ConnectionTypeLongPolling); err != nil {
		return nil, HandshakeFailedError{err}
	}
	if b.client.transportType() == ConnectionTypeWebsocket {
		if err := builder.AddSupportedConnectionType(ConnectionTypeWebsocket); err != nil {
			return nil, HandshakeFailedError{err}
		}
	}
	ms, err := builder.Build()
	if err != nil {
		return nil, HandshakeFailedError{err}
	}

	resp, err := b.request(ctx, ms)
	if err != nil {
		logger.WithError(err).Debug("error during request")
		return nil, HandshakeFailedError{err}
	}

	if len(resp) > 1 {
		return resp, HandshakeFailedError{ErrTooManyMessages}
	}

	var message Message
	for _, m := range resp {
		if m.Channel == MetaHandshake {
			message = m
		}
	}
	if message.Channel == emptyChannel {
		return resp, HandshakeFailedError{ErrBadChannel}
	}
	if !message.Successful {
		return resp, newHandshakeError(message.Error)
	}
	b.state.SetClientID(message.ClientID)
	_ = b.stateMachine.ProcessEvent(successfullyConnected)
	logger.WithField("duration", time.Since(start)).Debug("finishing")
	return resp, nil
}

// Connect sends the connect request to the Bayeux Server. The specification
// says that clients MUST maintain only one outstanding connect request. See
// https://docs.cometd.org/current/reference/#_bayeux_meta_connect
func (b *BayeuxClientT) Connect(ctx context.Context) ([]Message, error) {
	logger := b.logger.WithField("at", "connect")
	start := time.Now()
	logger.Debug("starting")
	clientID := b.state.GetClientID()
	if !b.stateMachine.IsConnected() || clientID == "" {
		return nil, ErrClientNotConnected
	}
	builder := NewConnectRequestBuilder()
	builder.AddClientID(clientID)
	_ = builder.AddConnectionType(b.client.transportType())
	ms, err := builder.Build()
	if err != nil {
		return nil, ConnectionFailedError{err}
	}

	resp, err := b.request(ctx, ms)
	if err != nil {
		logger.WithError(err).Debug("error during request")
		return nil, ConnectionFailedError{err}
	}

	for _, m := range resp {
		if m.Channel == MetaConnect && !m.Successful {
			return resp, ConnectionFailedError{ErrFailedToConnect}
		}
	}
	logger.WithField("duration", time.Since(start)).Debug("finishing")
	return resp, nil
}

// Subscribe issues a MetaSubscribe request to the server to subscribe to the
// channels in the subscriptions slice
func (b *BayeuxClientT) Subscribe(ctx context.Context, subscriptions []Channel) ([]Message, error) {
	logger := b.logger.WithField("at", "subscribe")
	start := time.Now()
	logger.Debug("starting")
	clientID := b.state.GetClientID()
	if !b.stateMachine.IsConnected() || clientID == "" {
		logger.Debug("cannot subscribe because client is not connected")
		return nil, SubscriptionFailedError{subscriptions, ErrClientNotConnected}
	}

	builder := NewSubscribeRequestBuilder()
	builder.AddClientID(clientID)
	for _, s := range subscriptions {
		if err := builder.AddSubscription(s); err != nil {
			return nil, SubscriptionFailedError{subscriptions, err}
		}
	}

	ms, err := builder.Build()
	if err != nil {
		return nil, SubscriptionFailedError{subscriptions, err}
	}

	resp, err := b.request(ctx, ms)
	if err != nil {
		return nil, SubscriptionFailedError{subscriptions, err}
	}

	for _, m := range resp {
		if m.Channel == MetaSubscribe && !m.Successful {
			return nil, SubscriptionFailedError{
				Channels: subscriptions,
				Err:      newSubscribeError(m.Error),
			}
		}
	}
	logger.WithField("duration", time.Since(start)).Debug("finishing")
	return resp, nil
}

// Unsubscribe issues a MetaUnsubscribe request to the server to subscribe to the
// channels in the subscriptions slice
func (b *BayeuxClientT) Unsubscribe(ctx context.Context, subscriptions []Channel) ([]Message, error) {
	clientID := b.state.GetClientID()
	if !b.stateMachine.IsConnected() || clientID == "" {
		return nil, UnsubscribeFailedError{subscriptions, ErrClientNotConnected}
	}

	builder := NewUnsubscribeRequestBuilder()
	builder.AddClientID(clientID)
	for _, s := range subscriptions {
		if err := builder.AddSubscription(s); err != nil {
			return nil, UnsubscribeFailedError{subscriptions, err}
		}
	}

	ms, err := builder.Build()
	if err != nil {
		return nil, UnsubscribeFailedError{subscriptions, err}
	}

	resp, err := b.request(ctx, ms)
	if err != nil {
		return nil, UnsubscribeFailedError{subscriptions, err}
	}

	for _, m := range resp {
		if m.Channel == MetaUnsubscribe && !m.Successful {
			return resp, UnsubscribeFailedError{
				Channels: subscriptions,
				Err:      newUnsubscribeError(m.Error),
			}
		}
	}
	return resp, nil
}

// Disconnect sends a /meta/disconnect request to the Bayeux server to
// terminate the session
func (b *BayeuxClientT) Disconnect(ctx context.Context) ([]Message, error) {
	clientID := b.state.GetClientID()
	if !b.stateMachine.IsConnected() || clientID == "" {
		return nil, DisconnectFailedError{ErrClientNotConnected}
	}

	builder := NewDisconnectRequestBuilder()
	builder.AddClientID(clientID)
	ms, err := builder.Build()
	if err != nil {
		return nil, DisconnectFailedError{err}
	}

	resp, err := b.request(ctx, ms)
	if err != nil {
		return nil, DisconnectFailedError{err}
	}

	for _, m := range resp {
		if m.Channel == MetaDisconnect && !m.Successful {
			return resp, DisconnectFailedError{nil}
		}
	}
	return resp, nil
}

// UseExtension adds the provided MessageExtender to the list of known
// extensions
func (b *BayeuxClientT) UseExtension(ext MessageExtender) error {
	for _, registered := range b.exts {
		if ext == registered {
			return AlreadyRegisteredError{ext}
		}
	}
	b.exts = append(b.exts, ext)
	return nil
}

func (b *BayeuxClientT) request(ctx context.Context, ms []Message) ([]Message, error) {
	for _, ext := range b.exts {
		for _, m := range ms {
			ext.Outgoing(&m)
		}
	}
	respMs, err := b.client.request(ctx, ms)
	if err != nil {
		return respMs, err
	}

	for _, ext := range b.exts {
		for _, m := range respMs {
			ext.Incoming(&m)
		}
	}
	return respMs, nil
}
