package sqlplugin

import (
	"context"
	"encoding/json"
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
	Cache         kafkadeduplication.Cache
}

type Window struct {
	InitFrom time.Time
	Lag      time.Duration
	windowfrom       time.Time
	windowto       time.Time

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

	var cache kafkadeduplication.Cache
	if len(saramaConfig.Brokers) > 0 {
		err := cache.CacheBuilder(saramaConfig)
		if err != nil {
			return nil, err
		}
	}

	sub := &Subscriber{
		DB:           s.DB,
		SelectQuery:  s.SelectQuery,
		config:       config,
		scyllaSchema: adapter,
		subscribeWg:  &sync.WaitGroup{},
		closing:      make(chan struct{}),
		logger:       logger,
		Args:         make(map[string]interface{}),
		Window:       s.Window,
		Cache:        cache,
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
	from := s.Window.InitFrom
	to := time.Now()
	if from.IsZero() && s.Cache.Count > 0 {
		var err error
		err, from = s.Cache.GetLastTimestamp()
		if err != nil {
			s.logger.Error("QueryContext error is not nil:", err, nil)
		}
	}
	for {
		s.Args["windowfrom"] = from
		s.Args["windowto"] = to
		s.query(ctx, out)
		delay := time.Until(to.Add(s.Window.Lag))
		if delay > 0 {
			time.Sleep(delay)
		}
		from = to
		to = time.Now()
	}
}

// query function for close rows connection
func (s *Subscriber) query(ctx context.Context, out chan *message.Message) {
	query, err := s.DB.PrepareNamed(s.SelectQuery)
	if err != nil {
		s.logger.Error("QueryContext Rows error is not nil:", err, nil)
		return
	}
	rows, err := query.Queryx(s.Args)
	defer func(rows *stdSQL.Rows) {
		err = rows.Close()
		if err != nil {
			s.logger.Error("QueryContext Rows error is not nil:", err, nil)
			return
		}
	}(rows)
	if err != nil {
		s.logger.Error("QueryContext Rows error is not nil:", err, nil)
		return
	}
	for rows.Next() {
		m := make(map[string]interface{})
		err = rows.MapScan(m)
		if err != nil {
			s.logger.Error("QueryContext Rows error is not nil:", err, nil)
			return
		}
		if s.Cache.EqualMsg(m) {
			s.logger.Error("Message is equal:", err, nil)
			continue
		}
		jsonString, _ := json.Marshal(m)
		msg := message.NewMessage(watermill.NewULID(), []byte(jsonString))
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
