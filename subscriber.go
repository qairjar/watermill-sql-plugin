package sqlplugin

import (
	"context"
	sql2 "database/sql"
	"errors"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	stdSQL "github.com/jmoiron/sqlx"
	"github.com/qairjar/kafka-deduplication"
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
	Args          map[string]interface{}
	Window        Window
	cache 		  []map[string]interface{}
}

type Window struct {
	InitFrom time.Duration
	Lag      time.Duration
}

// NewSubscriber create watermill subscriber module
func (s *Subscriber) NewSubscriber(adapter Adapter, logger watermill.LoggerAdapter, saramaConfig *kafkadeduplication.SaramaConfig) (*Subscriber, error) {
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
	cache

	if err != nil {
		return nil, err
	}

	sub := &Subscriber{
		config:       config,
		scyllaSchema: adapter,
		subscribeWg:  &sync.WaitGroup{},
		closing:      make(chan struct{}),
		logger:       logger,
		Cache: saramaConfig.Msgs,
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
	prep, err := s.DB.PrepareContext(ctx, s.SelectQuery)
	if err != nil {
		s.logger.Error("QueryContext error is not nil:", err, nil)
		return
	}
	defer func(prep *sql2.Stmt) {
		err := prep.Close()
		if err != nil {
			s.logger.Error("QueryContext error is not nil:", err, nil)
		}
	}(prep)
	if err != nil {
		return
	}
	var to time.Time
	for {
		s.query(ctx, prep, out)
		delay := time.Until(to.Add(s.Window.Lag))
		if delay > 0 {
			time.Sleep(delay)
		}
		s.Args["from"] = to
		to = time.Now()
		s.Args["to"] = time.Now()
	}
}

//query function for close rows connection
func (s *Subscriber) query(ctx context.Context, prep *sql2.Stmt, out chan *message.Message) {
	rows, err := prep.Query(s.Args)
	defer func(rows *sql2.Rows) {
		err = rows.Close()
		if err != nil {
			s.logger.Error("QueryContext Rows error is not nil:", err, nil)
		}
	}(rows)
	if err != nil {
		s.logger.Error("QueryContext Rows error is not nil:", err, nil)
	}
	for rows.Next() {
		msg, err := s.scyllaSchema.UnmarshalMessage(rows)
		if err != nil {
			s.logger.Error("QueryContext Rows error is not nil:", err, nil)
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
