package sqlplugin

import (
	"context"
	"database/sql"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
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
	DB                *sql.DB
	publishWg         *sync.WaitGroup
	closeCh           chan struct{}
	closed            bool
	initializedTopics sync.Map
	logger            watermill.LoggerAdapter
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

	return &Publisher{
		schemaAdapter: schema,
		publishWg:     new(sync.WaitGroup),
		closeCh:       make(chan struct{}),
		closed:        false,
		DB:            c.DB,
		logger:        logger,
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
	publishTracker := metric.Must(meter).NewInt64ValueRecorder("watermill_scylla_publish_ms")
	// Init context
	c.publishWg.Add(1)
	defer c.publishWg.Done()
	for _, msg := range messages {
		ctx := msg.Context()
		_, pubAttr := buildAttrs(ctx)
		spanCtx, span := tr.Start(ctx, "publish messages", trace.WithSpanKind(trace.SpanKindProducer))
		span.SetAttributes(pubAttr...)
		// Track processing complete
		publishStart := time.Now()
		err = c.query(spanCtx, topic, msg)
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
func (c *Publisher) query(ctx context.Context, topic string, msg *message.Message) error {
	var err error
	insertQuery, args, err := c.schemaAdapter.MappingData(c.Topic, msg)
	if err != nil {
		c.logger.Error("could not mapping message", err, watermill.LogFields{
			"topic": topic,
		})
		return err
	}
	c.logger.Trace("Inserting message to SQL", watermill.LogFields{
		"query": insertQuery,
		"args" : args,
	})
	_, err = c.DB.ExecContext(ctx, insertQuery, args...)
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
