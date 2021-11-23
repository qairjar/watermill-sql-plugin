package sqlplugin

import (
	"context"
	stdSQL "database/sql"
	"errors"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/qairjar/kafka-deduplication"
	"net/url"
	"sync"
	"time"
)

type Subscriber struct {
	closed        bool
	subscribeWg   *sync.WaitGroup
	closing       chan struct{}
	SelectQuery   string
	logger        watermill.LoggerAdapter
	consumerGroup string
	config        sql.SubscriberConfig
	DB            *stdSQL.DB
	scyllaSchema  Adapter
	TimeDuration  time.Duration
	Brokers       string
	KafkaTopic    string
}

var cacheBuilder kafkadeduplication.CacheBuilder

func (s *Subscriber) InitCache(connection string) (string, error) {
	uri, err := url.Parse(connection)
	if err != nil {
		return "", err
	}
	queryURI := uri.Query()
	window := kafkadeduplication.CacheBuilder{}
	if len(queryURI.Get("from-time")) > 0 {
		window.From, err = time.Parse(time.RFC3339, queryURI.Get("from-time"))
		if err != nil {
			return "", err
		}
	}
	queryURI.Del("from-time")

	if len(queryURI.Get("to-time")) > 0 {
		window.To, err = time.Parse(time.RFC3339, queryURI.Get("to-time"))
		if err != nil {
			return "", err
		}
	}
	if len(queryURI.Get("init-from")) > 0 {
		window.InitFrom, err = time.Parse(time.RFC3339, queryURI.Get("init-from"))
		if err != nil {
			return "", err
		}
	}
	queryURI.Del("init-from")
	cacheBuilder = window

	cacheBuilder.ArgsBuilder()

	return uri.String(), err
}

// NewSubscriber create watermill subscriber module
func (s *Subscriber) NewSubscriber(adapter Adapter, logger watermill.LoggerAdapter) (*Subscriber, error) {
	if s.DB == nil {
		return nil, errors.New("db is nil")
	}
	config := setDefaults()
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	if adapter == nil {
		var schema Schema
		adapter = schema
	}

	sub := &Subscriber{
		config:       config,
		scyllaSchema: adapter,
		subscribeWg:  &sync.WaitGroup{},
		closing:      make(chan struct{}),
		logger:       logger,
	}
	return sub, nil
}

func (s Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan *message.Message)

	s.subscribeWg.Add(1)
	go func() {
		s.consume(ctx, out)
		close(out)
		cancel()
	}()
	return out, nil
}
func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true

	close(s.closing)
	s.subscribeWg.Wait()
	return nil
}

func (s *Subscriber) consume(
	ctx context.Context,
	out chan *message.Message,
) {
	for {
		s.query(ctx, out)
		delay := time.Until(cacheBuilder.To) + cacheBuilder.Lag
		if delay > 0 {
			time.Sleep(delay)
		}
		cacheBuilder.From = cacheBuilder.To
		cacheBuilder.To = time.Now()
	}
}
func (s *Subscriber) query(ctx context.Context,
	out chan *message.Message) {
	ctx, cancel := context.WithTimeout(ctx, 55*time.Second)
	defer cancel()
	prep, err := s.DB.PrepareContext(ctx, s.SelectQuery)
	if err != nil {
		s.logger.Error("QueryContext error is not nil:", err, nil)
		return
	}
	defer func(prep *stdSQL.Stmt) {
		err = prep.Close()
		if err != nil {
			s.logger.Error(err.Error(), err, nil)
		}
	}(prep)

	rows, err := prep.Query(cacheBuilder.From, cacheBuilder.To)

	for rows.Next() {
		msg, err := s.scyllaSchema.UnmarshalMessage(rows)

		if err != nil {
			fmt.Println("QueryContext error is not nil:", err)
		}
		s.sendMessage(ctx, msg, out)
	}
}

// sendMessages sends messages on the output channel.
func (s *Subscriber) sendMessage(
	ctx context.Context,
	msg *message.Message,
	out chan *message.Message,
) (acked bool) {
	msgCtx, cancel := context.WithCancel(ctx)
	msg.SetContext(msgCtx)
	defer cancel()
	logger := s.logger
ResendLoop:
	for {
		select {
		case out <- msg:

		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			return false

		case <-ctx.Done():
			logger.Info("Discarding queued message, context canceled", nil)
			return false
		}

		select {
		case <-msg.Acked():
			logger.Debug("Message acked by subscriber", nil)
			return true

		case <-msg.Nacked():
			// message nacked, try resending
			logger.Debug("Message nacked, resending", nil)
			msg = msg.Copy()
			msg.SetContext(msgCtx)

			if s.config.ResendInterval != 0 {
				time.Sleep(s.config.ResendInterval)
			}

			continue ResendLoop

		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			return false

		case <-ctx.Done():
			logger.Info("Discarding queued message, context canceled", nil)
			return false
		}
	}
}

func setDefaults() sql.SubscriberConfig {
	var c sql.SubscriberConfig
	if c.PollInterval == 0 {
		c.PollInterval = time.Second
	}
	if c.ResendInterval == 0 {
		c.ResendInterval = time.Second
	}
	if c.RetryInterval == 0 {
		c.RetryInterval = time.Second
	}
	if c.BackoffManager == nil {
		c.BackoffManager = sql.NewDefaultBackoffManager(c.PollInterval, c.RetryInterval)
	}
	return c
}
