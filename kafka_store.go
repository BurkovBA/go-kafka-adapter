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
	BootstrapServers string // kafka brokers to negotiate the protocol with upon consumer bootstrap, default: "localhost:29092"
	Topic            string // name of the topic that stores our data, default: "myTopic"
	ConsumerGroupId  string // id of our consumer group, default: "goConsumerGroup"
}

func NewKafkaStore(BootstrapServers string, Topic string, consumerGroupId string) *KafkaStore {
	kafkaStore := &KafkaStore{}

	kafkaStore.BootstrapServers = BootstrapServers
	kafkaStore.Topic = Topic
	kafkaStore.consumerGroupId = consumerGroupId

	return kafkaStore
}

func (kafkaStore KafkaStore) LoadMeta(ctx context.Context, callback func(reader io.Reader) error) error {
	// writer side of the pipe is ours, reader is passed to the callback
	reader, writer := io.Pipe()

	// bootstrap the consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaStore.BootstrapServers,
		"group.id":          kafkaStore.consumerGroupId,
		"auto.offset.reset": "earliest",
	})
	defer consumer.Close()
	if err != nil {
		return err
	}
	fmt.Println("Created a new consumer")

	// subscribe to topics
	err = consumer.SubscribeTopics([]string{kafkaStore.Topic}, nil)
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

	err = kafkaStore.pollReadMessage(consumer, writer)
	if err != nil {
		return err
	}

	waitgroup.Wait()

	return nil
}

// Implementation of Kafka consumer polling operation via a low-level Poll() function.
// This implementation we won't be able to Seek/Assign the offset.
// Mostly stolen from: https://github.com/martinhynar/kafka-consumer/blob/AssignedPartitions/kafka-consumer.go
func (kafkaStore KafkaStore) poll(consumer *kafka.Consumer, writer *io.PipeWriter) error {
	var run bool
	for run == true {

		event := consumer.Poll(10000)
		if event == nil {
			continue
		}

		switch e := event.(type) {
		case kafka.AssignedPartitions:
			fmt.Println("Partitions assigned")
			for _, ap := range e.Partitions {
				fmt.Printf("%s[%d]@%v", ap.Topic, ap.Partition, ap.Offset)
			}
			consumer.Assign(e.Partitions)

			var start kafka.Offset = kafka.OffsetBeginning
			var searchTP []kafka.TopicPartition = make([]kafka.TopicPartition, len(e.Partitions))
			for i, ap := range e.Partitions {
				searchTP[i] = kafka.TopicPartition{Topic: &kafkaStore.Topic, Partition: ap.Partition, Offset: start}
			}
			timeoutMs := 5000
			rewindTP, err := consumer.OffsetsForTimes(searchTP, timeoutMs)
			if err != nil {
				fmt.Printf("Timestamp offset search error: %v\n", err)
			} else {
				err = consumer.Assign(rewindTP)
				fmt.Println("Partition re-assignment")
				for _, ap := range rewindTP {
					fmt.Printf("%s[%d]@%v", ap.Topic, ap.Partition, ap.Offset)
				}
				if err != nil {
					fmt.Printf("Partition assignment error: %v\n", err)
				}
			}
		case kafka.RevokedPartitions:
			fmt.Printf("%% %v\n", e)
			consumer.Unassign()

		case *kafka.Message:
			fmt.Printf("kafka@%d : %s", milliseconds(&e.Timestamp), string(e.Value))
		case kafka.PartitionEOF:
			fmt.Printf("%% Reached %v\n", e)
		case kafka.Error:
			fmt.Printf("%% Error: %v\n", e)
			run = false
		default:
			fmt.Printf("Ignored %v\n", e)
		}
	}

	return nil
}

func milliseconds(moment *time.Time) int64 {
	return moment.UnixNano() / int64(time.Millisecond)
}

// Implementation of Kafka consumer polling operation via a high-level ReadMessage() function.
// Apparently with this implementation we won't be able to Seek/Assign the offset.
func (kafkaStore KafkaStore) pollReadMessage(consumer *kafka.Consumer, writer *io.PipeWriter) error {
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
		_, highWatermark, err := consumer.QueryWatermarkOffsets(kafkaStore.Topic, msg.TopicPartition.Partition, 10000)
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
