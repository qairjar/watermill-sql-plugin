package sqlplugin

import (
	"context"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/qairjar/watermill-sql-plugin/cache"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/trace"
	"sync"
	"time"
)

var (
	ErrPublisherClosed = errors.New("publisher is closed")
)

// Publisher inserts the Messages as rows into SQL table.
type Publisher struct {
	Topic             string
	schemaAdapter     Adapter
	DB                *sqlx.DB
	publishWg         *sync.WaitGroup
	closeCh           chan struct{}
	closed            bool
	initializedTopics sync.Map
	logger            watermill.LoggerAdapter
	Query             string
	Select            string
	Cache             *cache.Cache
}

func (c *Publisher) setDefaults() {
	var schema Schema
	c.schemaAdapter = schema
}

// NewPublisher crete pub module
func (c *Publisher) NewPublisher(schema Adapter, logger watermill.LoggerAdapter) (*Publisher, error) {
	if c.DB == nil {
		return nil, errors.New("db is nil")
	}
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	if schema == nil {
		var schemaAdapter Schema
		schema = schemaAdapter
	}

	pubCache, err := cache.InitCache(c.Select, c.DB)

	if err != nil {
		return nil, err
	}

	return &Publisher{
		schemaAdapter: schema,
		publishWg:     new(sync.WaitGroup),
		closeCh:       make(chan struct{}),
		closed:        false,
		DB:            c.DB,
		logger:        logger,
		Query:         c.Query,
		Cache: pubCache,
	}, nil
}

// Publish inserts the messages as rows into the MessagesTable.
func (c *Publisher) Publish(topic string, messages ...*message.Message) (err error) {
	if c.closed {
		return ErrPublisherClosed
	}
	// Setup tracing
	tr := otel.Tracer("sql")
	// Setup metrics
	meter := global.Meter("sql")
	publishTracker := metric.Must(meter).NewInt64Counter("watermill_scylla_publish_ms")
	// Init context
	c.publishWg.Add(1)
	defer c.publishWg.Done()
	for _, msg := range messages {
		ctx := msg.Context()
		_, pubAttr := buildAttrs(ctx)
		_, span := tr.Start(ctx, "publish messages", trace.WithSpanKind(trace.SpanKindProducer))
		span.SetAttributes(pubAttr...)
		// Track processing complete
		publishStart := time.Now()
		err = c.query(topic, msg)
		if err != nil {
			span.SetStatus(codes.Error, "failed to insert query into scylla")
			span.RecordError(err)
			return
		}
		span.End()
		meter.RecordBatch(
			ctx,
			pubAttr,
			publishTracker.Measurement(time.Since(publishStart).Milliseconds()),
		)
	}
	return nil
}
func (c *Publisher) query(topic string, msg *message.Message) error {
	args, err := c.schemaAdapter.MappingData(topic, msg)
	if err != nil {
		c.logger.Error("could not mapped message", err, watermill.LogFields{
			"topic": topic,
		})
		return err
	}
	eq := c.Cache.EqualMsg(args)
	if eq {
		return nil
	}
	_, err = c.DB.NamedExec(c.Query, args)
	if err != nil {
		c.logger.Error("could not insert message as row", err, watermill.LogFields{
			"topic": topic,
		})
		return err
	}
	return nil
}

// buildAttrs build otel attributes from watermill context data
func buildAttrs(ctx context.Context) (processor, publisher []attribute.KeyValue) {
	handler := attribute.String("watermill_handler", message.HandlerNameFromCtx(ctx))
	proAttrs := []attribute.KeyValue{handler}
	pubAttrs := []attribute.KeyValue{handler, attribute.String("kafka_topic", message.PublishTopicFromCtx(ctx))}
	return proAttrs, pubAttrs
}

// Close closes the publisher, which means that all Publish calls called before are finished.
func (c *Publisher) Close() error {
	if c.closed {
		return nil
	}

	c.closed = true

	close(c.closeCh)
	c.publishWg.Wait()

	return nil
}
