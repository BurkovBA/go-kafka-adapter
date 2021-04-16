package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaStore struct {
	bootstrapServers string // kafka brokers to negotiate the protocol with upon consumer bootstrap, default: "localhost:29092"
	topic            string // name of the topic that stores out data, default: "myTopic"
	consumerGroupId  string // id of our consumer group, default: "goConsumerGroup"
}

func NewKafkaStore(bootstrapServers string, topic string, consumerGroupId string) *KafkaStore {
	kafkaStore := &KafkaStore{}

	kafkaStore.bootstrapServers = bootstrapServers
	kafkaStore.topic = topic
	kafkaStore.consumerGroupId = consumerGroupId

	return kafkaStore
}

func (kafkaStore KafkaStore) LoadMeta(ctx context.Context, callback func(reader io.Reader) error) error {
	// writer side of the pipe is ours, reader is passed to the callback
	reader, writer := io.Pipe()

	// bootstrap the consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaStore.bootstrapServers,
		"group.id":          kafkaStore.consumerGroupId,
		"auto.offset.reset": "earliest",
	})
	defer consumer.Close()
	if err != nil {
		return err
	}
	fmt.Println("Created a new consumer")

	// subscribe to topics
	err = consumer.SubscribeTopics([]string{kafkaStore.topic}, nil)
	if err != nil {
		return err
	}
	fmt.Println("Subscribed to topics")

	// initialze the callback goroutine
	var waitgroup sync.WaitGroup
	waitgroup.Add(1)
	go func() {
		defer waitgroup.Done()
		err2 := callback(reader)
		if err2 != nil {
			panic(err2)
		}
	}()

	// iteratively feed messages from kafka topic into the pipe
	for {
		fmt.Println("Reading a message from kafka")
		msg, err := consumer.ReadMessage(time.Second * 10) // use -1 to eliminate timeout and wait indefinitely
		if err != nil {
			return err
		}

		fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		_, err = writer.Write(msg.Value)
		if err != nil {
			return err
		}

		// get the offset of the last readable (i.e. fully replicated) message - highWatermark;
		// see: https://github.com/confluentinc/confluent-kafka-go/issues/557
		_, highWatermark, err := consumer.QueryWatermarkOffsets(kafkaStore.topic, msg.TopicPartition.Partition, 10000)
		if err != nil {
			return err
		}

		// close the writer, if the consumer's offset has reached highWatermark
		currentOffset := int64(msg.TopicPartition.Offset)
		fmt.Printf("currentOffset = %d, highWatermark = %d\n", currentOffset, highWatermark)
		if highWatermark == currentOffset+1 {
			writer.Close()
			return nil
		}
	}

	waitgroup.Wait()
}

func (kafkaStore KafkaStore) ReplaceMeta(ctx context.Context, callback func(writer io.Writer) error) error {
	return errors.New("not implemented")
}

func (kafkaStore KafkaStore) AppendMeta(ctx context.Context, callback func(writer io.Writer) error) error {
	// TODO: implement custom kafka writer and pass it to callback

	// writer := io.Writer()

	// callback(writer)

	return errors.New("not implemented")
}
