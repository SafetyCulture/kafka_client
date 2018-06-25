package kafka

import (
	"context"
	"errors"
	"time"

	kgo "github.com/segmentio/kafka-go"
)

type Message struct {
	Key   []byte
	Value []byte
}

type MapFunc func([]byte) (interface{}, error)

type KafkaClient interface {
	Export(string, []byte, []byte) error
	SubscribeAsync(string, string) (<-chan []byte, <-chan error)
	MapAsync(string, string, MapFunc) (<-chan interface{}, <-chan error)
	CreateQueue(string, time.Duration) *Queue

	// Shutdown gracefully stops the client - let already queued messages be processed first
	Shutdown()
}

func NewClient(brokers ...string) (client *Client, err error) {
	client = &Client{
		brokers:    brokers,
		inchannels: make(map[string]*Queue),
	}

	return client, nil
}

type Client struct {
	brokers    []string
	inchannels map[string]*Queue
}

type Queue struct {
	open         bool
	batch        []kgo.Message
	ch           chan<- Message
	er           chan error
	batchtimeout time.Duration

	writer *kgo.Writer
}

func (q *Queue) Errors() <-chan error {
	return q.er
}

func (q *Queue) Push(msg Message) error {
	if !q.open {
		return errors.New("queue is closed")
	}

	q.ch <- msg
	return nil
}

func (c *Client) CreateQueue(topic string, batchtimeout time.Duration) *Queue {
	if q, ok := c.inchannels[topic]; ok {
		return q
	}

	errs := make(chan error)
	msgs := make(chan Message)
	queue := &Queue{
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
		// queue.batch = []kgo.Message{}
		go func() {
			batched := time.Now()
			for {
				if time.Since(batched) > queue.batchtimeout {
					batched = time.Now()
					// MISSING finishing of bach on SHUTDOWN
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

	c.inchannels[topic] = queue
	return queue
}

func (c *Client) Export(topic string, key []byte, value []byte) error {
	kwriter := kgo.NewWriter(kgo.WriterConfig{
		Brokers: c.brokers,
		Topic:   topic,
	})
	defer kwriter.Close()

	err := kwriter.WriteMessages(context.Background(), kgo.Message{
		Key:   key,
		Value: value,
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) SubscribeAsync(group string, topic string) (<-chan []byte, <-chan error) {
	reader := kgo.NewReader(kgo.ReaderConfig{
		Brokers: c.brokers,
		GroupID: group,
		Topic:   topic,
	})

	items := make(chan []byte)
	errs := make(chan error)

	go func() {
		for {
			m, err := reader.ReadMessage(context.Background())
			if err != nil {
				errs <- err
			}
			items <- m.Value
		}
	}()

	return items, errs
}

func (c *Client) MapAsync(group string, topic string, f MapFunc) (<-chan interface{}, <-chan error) {
	items := make(chan interface{})
	errs := make(chan error)

	msgs, e := c.SubscribeAsync(group, topic)

	go func() {
		for err := range e {
			errs <- err
		}
	}()

	go func() {
		for msg := range msgs {
			v, e := f(msg)
			if e != nil {
				errs <- e
			} else {
				items <- v
			}
		}
	}()

	return items, errs
}

// Shutdown gracefully stops the client - let already queued messages be processed first
func (c *Client) Shutdown() {
	for {
		for topic, q := range c.inchannels {
			if q.open {
				c.inchannels[topic].open = false
			}
			if len(q.ch) == 0 && len(q.batch) == 0 {
				close(q.ch)
				delete(c.inchannels, topic)

				q.writer.Close()
			}
		}

		if len(c.inchannels) == 0 { // Job done. Return.
			return
		}
	}
}
