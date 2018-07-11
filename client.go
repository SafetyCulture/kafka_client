package kafka

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"time"

	kgo "github.com/segmentio/kafka-go"
)

type Message struct {
	Key   []byte
	Value []byte
}

type Offset int

const (
	OffsetEarliest Offset = iota
	OffsetLatest
)

type ReadConfiguration struct {
	Group          string
	CommitInterval time.Duration
	Offset         Offset
}

type MapFunc func([]byte)

type KafkaClient interface {
	CreateWriteQueue(topic string, batchtimeout time.Duration) WriteQueue
	CreateReadQueue(topic string, configuration ReadConfiguration) (ReadQueue, error)

	// Shutdown gracefully stops the client - let already queued messages be processed first
	Shutdown()
}

func NewClient(brokers ...string) (client *Client) {
	client = &Client{
		brokers:     brokers,
		outchannels: make(map[string]*WQueue),
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			client.Shutdown()
		}
	}()

	return client
}

type Client struct {
	brokers     []string
	outchannels map[string]*WQueue
	inchannels  []*RQueue
}

type WriteQueue interface {
	Errors() <-chan error
	Push(Message) error
}

type WQueue struct {
	open         bool
	batch        []kgo.Message
	ch           chan<- Message
	er           chan error
	batchtimeout time.Duration

	writer *kgo.Writer
}

func (q *WQueue) Errors() <-chan error {
	return q.er
}

func (q *WQueue) Push(msg Message) error {
	if !q.open {
		return errors.New("queue is closed")
	}

	q.ch <- msg
	return nil
}

func (c *Client) CreateWriteQueue(topic string, batchtimeout time.Duration) WriteQueue {
	if q, ok := c.outchannels[topic]; ok {
		return q
	}

	errs := make(chan error)
	msgs := make(chan Message)
	queue := &WQueue{
		open:         true,
		er:           errs,
		ch:           msgs,
		batchtimeout: batchtimeout,
	}

	queue.writer = kgo.NewWriter(kgo.WriterConfig{
		Brokers:      c.brokers,
		Topic:        topic,
		BatchTimeout: 1 * time.Microsecond,
	})

	go func() {
		go func() {
			batched := time.Now()
			for {
				if time.Since(batched) > queue.batchtimeout {
					batched = time.Now()
					if len(queue.batch) > 0 {
						err := queue.writer.WriteMessages(context.Background(), queue.batch...)
						if err != nil {
							errs <- err
						}
					}
					queue.batch = []kgo.Message{}
				} else {
					time.Sleep(queue.batchtimeout - time.Since(batched))
				}
			}
		}()

		for msg := range msgs {
			queue.batch = append(queue.batch, kgo.Message{
				Key:   msg.Key,
				Value: msg.Value,
			})
		}
	}()

	c.outchannels[topic] = queue
	return queue
}

type ReadQueue interface {
	SubscribeAsync(f MapFunc) error
	Errors() <-chan error
}

type RQueue struct {
	errs chan error

	cfg    ReadConfiguration
	reader *kgo.Reader
}

func (q *RQueue) Errors() <-chan error {
	return q.errs
}

func (c *Client) CreateReadQueue(topic string, config ReadConfiguration) (ReadQueue, error) {
	if config.Offset == OffsetLatest && len(config.Group) > 0 {
		return nil, errors.New("cant use offset with group, please use either one")
	}

	reader := kgo.NewReader(kgo.ReaderConfig{
		Brokers:        c.brokers,
		Topic:          topic,
		GroupID:        config.Group,
		CommitInterval: config.CommitInterval,
	})
	if config.Offset == OffsetLatest {
		reader.SetOffset(-2)
	}

	q := &RQueue{
		errs:   make(chan error),
		cfg:    config,
		reader: reader,
	}

	c.inchannels = append(c.inchannels, q)

	return q, nil
}

func (q *RQueue) SubscribeAsync(f MapFunc) error {
	if f == nil {
		return errors.New("Map function cant be nil")
	}

	go func() {
		ctx := context.Background()
		for {
			m, err := q.reader.FetchMessage(ctx)
			if err != nil {
				q.errs <- err
			}
			if q.cfg.CommitInterval <= 0 {
				err := q.reader.CommitMessages(ctx, m)
				if err != nil {
					q.errs <- err
					break
				}
			}

			f(m.Value)
		}
	}()

	return nil
}

// Shutdown gracefully stops the client - let already queued messages be processed first
func (c *Client) Shutdown() {
	for {
		for topic, q := range c.outchannels {
			if q.open {
				c.outchannels[topic].open = false
			}
			if len(q.ch) == 0 && len(q.batch) == 0 {
				close(q.ch)
				delete(c.outchannels, topic)

				q.writer.Close()
			}
		}

		for _, readQueu := range c.inchannels {
			readQueu.reader.Close()
		}

		if len(c.outchannels) == 0 { // Job done. Return.
			return
		}
	}
}
