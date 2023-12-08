package gobayeux

import (
	"context"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

// Client is a high-level abstraction
type Client struct {
	client                    *BayeuxClient
	subscriptions             *subscriptionsMap
	logger                    Logger
	subscribeRequestChannel   chan subscriptionRequest
	unsubscribeRequestChannel chan Channel
	connectRequestChannel     chan struct{}
	connectMessageChannel     chan []Message
	handshakeRequestChannel   chan struct{}
	shutdown                  chan struct{}
	ignoreError               IgnoreErrorFunc
}

// IgnoreErrorFunc is a callback function that inspects an error and determines
// if it can be safely ignored when subscribing and unsubscribing.
type IgnoreErrorFunc func(error) bool

// Options stores the available configuration options for a Client
type Options struct {
	Logger      Logger
	Client      *http.Client
	Transport   http.RoundTripper
	IgnoreError IgnoreErrorFunc
}

// Option defines the type passed into NewClient for configuration
type Option func(*Options)

// WithLogger returns an Option with logger.
func WithLogger(logger Logger) Option {
	return func(options *Options) {
		options.Logger = logger
	}
}

// WithFieldLogger returns an Option with logger.
func WithFieldLogger(logger logrus.FieldLogger) Option {
	return func(options *Options) {
		options.Logger = &wrappedFieldLogger{logger}
	}
}

// WithHTTPClient returns an Option with custom http.Client.
func WithHTTPClient(client *http.Client) Option {
	return func(options *Options) {
		options.Client = client
	}
}

// WithHTTPTransport returns an Option with custom http.RoundTripper.
func WithHTTPTransport(transport http.RoundTripper) Option {
	return func(options *Options) {
		options.Transport = transport
	}
}

// WithIgnoreError takes a function that will be called whenever an error is
// returned while subscribing or unsubscribing. If the function returns true,
// the error will not be considered fatal the the event loop will continue.
//
// The default is to stop when an error is received.
func WithIgnoreError(f IgnoreErrorFunc) Option {
	return func(options *Options) {
		options.IgnoreError = f
	}
}

// NewClient creates a new high-level client
func NewClient(serverAddress string, opts ...Option) (*Client, error) {
	options := &Options{}

	// Apply passed opts
	for _, opt := range opts {
		if opt != nil {
			opt(options)
		}
	}

	if options.Logger == nil {
		options.Logger = newNullLogger()
	}

	if options.IgnoreError == nil {
		options.IgnoreError = func(err error) bool {
			return false
		}
	}

	bc, err := NewBayeuxClient(options.Client, options.Transport, serverAddress, options.Logger)
	if err != nil {
		return nil, err
	}

	return &Client{
		client:                    bc,
		subscriptions:             newSubscriptionsMap(),
		subscribeRequestChannel:   make(chan subscriptionRequest, 10),
		unsubscribeRequestChannel: make(chan Channel, 10),
		connectRequestChannel:     make(chan struct{}, 1),
		connectMessageChannel:     make(chan []Message, 5),
		handshakeRequestChannel:   make(chan struct{}),
		shutdown:                  make(chan struct{}),
		logger:                    options.Logger,
		ignoreError:               options.IgnoreError,
	}, nil
}

// Subscribe queues a request to subscribe to a new channel from the server
func (c *Client) Subscribe(ch Channel, receiving chan []Message) {
	c.subscribeRequestChannel <- subscriptionRequest{ch, receiving}
}

// Unsubscribe queues a request to unsubscribe from a channel on the server
func (c *Client) Unsubscribe(ch Channel) {
	c.unsubscribeRequestChannel <- ch
}

// Start begins the background process that talks to the server
func (c *Client) Start(ctx context.Context) <-chan error {
	errors := make(chan error)
	go c.start(ctx, errors)
	return errors
}

// Disconnect issues a /meta/disconnect request to the Bayeux server and then
// cleans up channels and our timer.
func (c *Client) Disconnect(ctx context.Context) error {
	_, err := c.client.Disconnect(ctx)
	close(c.subscribeRequestChannel)
	close(c.unsubscribeRequestChannel)
	close(c.connectRequestChannel)
	close(c.connectMessageChannel)
	close(c.handshakeRequestChannel)
	return err
}

// Publish is not yet implemented. When implemented, it will - in a separate thread
// from the polling task - publish messages to the Bayeux Server.
//
// See also: https://docs.cometd.org/current/reference/#_two_connection_operation
func (c *Client) Publish(ctx context.Context, messages []Message) error {
	// TODO:
	// * Locking mechanism to ensure only one outstanding Publish request at a
	//   time
	// * Ensure that this separate from Start()/poll()
	// * Implement Publish() in *BayeuxClient
	panic("Publish() is not yet implemented")
}

// UseExtension adds the provided MessageExtender as an extension for use with
// this Client session.
//
// See also: https://docs.cometd.org/current/reference/#_bayeux_ext
func (c *Client) UseExtension(ext MessageExtender) error {
	return c.client.UseExtension(ext)
}

func (c *Client) start(ctx context.Context, errors chan error) {
	logger := c.logger.WithField("at", "start")
	if _, err := c.client.Handshake(ctx); err != nil {
		errors <- err
		return
	}

	_ = c.subscriptions.Add(MetaConnect, c.connectMessageChannel)

	logger.Debug("starting long-polling loop")
	if err := c.poll(ctx, errors); err != nil {
		errors <- err
		return
	}

	if _, err := c.client.Disconnect(ctx); err != nil {
		errors <- err
		return
	}
}

func (c *Client) poll(ctx context.Context, errors chan<- error) error {
	logger := c.logger.WithField("at", "poll")
_poll_loop:
	for {
		logger.Debug("in polling loop")
		select {
		case <-c.shutdown: // When the user calls the Disconnect() method
			logger.Debug("shutting down due to Disconnect()")
			break _poll_loop
		case <-ctx.Done(): // When the user cancels the Start() context
			if err := ctx.Err(); err != nil {
				logger.WithError(err).Debug("shutting down due to error")
				return err
			}
			logger.Debug("shutting down due to cancelled context")
			break _poll_loop
		case subReq := <-c.subscribeRequestChannel:
			logger.Debug("got subscription requests")
			// Let's attempt to drain the channel before sending a
			// /meta/unsubscribe request to more efficiently use HTTP
			// requests
			subReqs, channels := c.getSubscriptionRequests()
			subReqs = append(subReqs, subReq)
			channels = append(channels, subReq.subscription)
			// TODO: Find a way to consolidate this logic and the logic in
			// start()
			if _, err := c.client.Subscribe(ctx, channels); err != nil {
				if c.ignoreError(err) {
					errors <- err
					continue
				}

				return err
			}

			for _, subReq := range subReqs {
				if err := c.subscriptions.Add(subReq.subscription, subReq.msgChan); err != nil {
					if c.ignoreError(err) {
						errors <- err
						continue
					}

					return err
				}
			}

			c.enqueueConnectRequest()

		case unsubReq := <-c.unsubscribeRequestChannel:
			logger.Debug("got unsubscribe requests")
			channels := c.getUnsubscriptionRequests()
			channels = append(channels, unsubReq)
			if _, err := c.client.Unsubscribe(ctx, channels); err != nil {
				if c.ignoreError(err) {
					errors <- err
					continue
				}

				return err
			}

			for _, channel := range channels {
				c.subscriptions.Remove(channel)
			}

		case <-c.handshakeRequestChannel:
			logger.Debug("re-handshaking")
			if _, err := c.client.Handshake(ctx); err != nil {
				return err
			}
			c.enqueueConnectRequest()
		case ms := <-c.connectMessageChannel:
			logger.Debug("handling messages from /meta/connect")
			for _, m := range ms {
				if m.Advice.ShouldHandshake() {
					logger.Debug("queueing new handshake request")
					c.handshakeRequestChannel <- struct{}{}
				}
				interval := m.Advice.IntervalAsDuration()
				go func() {
					c.logger.WithField("interval", interval).Debug("waiting per advice")
					<-time.After(interval)
					c.enqueueConnectRequest()
				}()
			}

		case <-c.connectRequestChannel:
			logger.Debug("checking for new messages")
			ms, err := c.client.Connect(ctx)
			if err != nil {
				logger.WithError(err).Debug("error in /meta/connect")
				return err
			}
			batch := make([]Message, 0)
			lastChannel := emptyChannel
			logger.Debug("delivering messages")
			for _, m := range ms {
				switch lastChannel {
				case emptyChannel:
					lastChannel = m.Channel
					batch = append(batch, m)
				case m.Channel:
					batch = append(batch, m)
				default:
					msgChan, err := c.subscriptions.Get(lastChannel)
					if err != nil {
						return err
					}
					logger.WithField("channel", lastChannel).Debug("sending batch")
					msgChan <- batch
					lastChannel = m.Channel
					batch = append([]Message(nil), m)
				}
			}

		default:
			c.enqueueConnectRequest()
		}
	}
	return nil
}

func (c *Client) getSubscriptionRequests() ([]subscriptionRequest, []Channel) {
	subscriptionRequests := make([]subscriptionRequest, 0)
	channels := make([]Channel, 0)

_get_subs_for_loop:
	for {
		select {
		case req := <-c.subscribeRequestChannel:
			subscriptionRequests = append(subscriptionRequests, req)
			channels = append(channels, req.subscription)
		default:
			break _get_subs_for_loop
		}
	}
	return subscriptionRequests, channels
}

func (c *Client) enqueueConnectRequest() {
	logger := c.logger.WithField("at", "enqueueConnectRequest")
	select {
	case c.connectRequestChannel <- struct{}{}:
		logger.Debug("queued next /meta/connect request")
	default:
		logger.Debug("/meta/connect request queue full")
	}
}

func (c *Client) getUnsubscriptionRequests() []Channel {
	unsubscriptionRequests := make([]Channel, 0)

_get_unsubs_for_loop:
	for {
		select {
		case req := <-c.unsubscribeRequestChannel:
			unsubscriptionRequests = append(unsubscriptionRequests, req)
		default:
			break _get_unsubs_for_loop
		}
	}
	return unsubscriptionRequests
}

type subscriptionRequest struct {
	subscription Channel
	msgChan      chan []Message
}
