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

// Kafka entities:
// - broker - single instance of Kakfa; you might have a cluster with e.g. 3 Kafka brokers on 3 separate servers for HA
// - topic - a single message queue, e.g. 'flats'
// - partition - you can shard topic by some criterion into partitions, e.g. split 'flats' into 'Moscow', 'St.Petersburg', 'Novosibirsk' partitions
// - replica - each partition can have a master and several replicas, each replica on ots broker; you can set up something like sync/async replication between them
// - consumer - a process that reads messages from Kafka; it can listen to multiple topics and within a topic to several partitions
// - consumer group - a set of (possibly interchangeable) consumers which have a single offset per topic partition
// - offset - the incremental index of a message, unique within a topic partition for a consumer group
// - producer - a process that writes messages into Kafka
// See: https://www.oreilly.com/library/view/kafka-the-definitive/9781491936153/ch04.html
type KafkaStore struct {
	BootstrapServers string // kafka brokers to negotiate the protocol with upon consumer bootstrap, default: "localhost:29092"
	Topic            string // name of the topic that stores our data, default: "myTopic"
	ConsumerGroupId  string // id of our consumer group, default: "goConsumerGroup"
}

func NewKafkaStore(BootstrapServers string, Topic string, ConsumerGroupId string) *KafkaStore {
	kafkaStore := &KafkaStore{}

	kafkaStore.BootstrapServers = BootstrapServers
	kafkaStore.Topic = Topic
	kafkaStore.ConsumerGroupId = ConsumerGroupId

	return kafkaStore
}

func (kafkaStore KafkaStore) LoadMeta(ctx context.Context, callback func(reader io.Reader) error) error {
	// writer side of the pipe is ours, reader is passed to the callback
	reader, writer := io.Pipe()

	// bootstrap the consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaStore.BootstrapServers,
		"group.id":          kafkaStore.ConsumerGroupId,
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
			switch err2 {
			case io.EOF:
				return
			default:
				panic(err2)
			}
		}
	}()

	err = kafkaStore.readMessage(consumer, writer)
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
	fmt.Println("Entering poll()")

	run := true
	for run == true {
		fmt.Println("Running an iteration of poll")
		event := consumer.Poll(10000)
		if event == nil {
			fmt.Println("Timeout in Poll(), Event is nil")
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
			run = false
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

// Reset the offsets of topic partitions, the consumer listens from, to the beginning.
//
// Kafka has lower-level and higher-level APIs for assigning consumers to partitions:
// - Assign() manually assigns a consumer to a specific partition of a topic.
// - Subscirbe() assigns a consumer to a topic and dynamically rebalances consumers in
//   that consumer group between partitions, should need arise. This is the preferred way.
func (kafkaStore KafkaStore) resetTopicPartitionsOffsets(consumer *kafka.Consumer) error {
	// get current topic partitions
	metadata, err := consumer.GetMetadata(&kafkaStore.Topic, false, 10000)

	// define that we want the earliest offsets available for each topic partition
	var modifiedTopicPartitions []kafka.TopicPartition = make([]kafka.TopicPartition, len(metadata.Topics[kafkaStore.Topic].Partitions))
	var start kafka.Offset = kafka.OffsetBeginning
	for index, partitionMetadata := range metadata.Topics[kafkaStore.Topic].Partitions {
		modifiedTopicPartitions[index] = kafka.TopicPartition{Topic: &kafkaStore.Topic, Partition: partitionMetadata.ID, Offset: start}
		fmt.Printf("resetTopicPartitionsOffsets() preparing to re-assign offsets: %s[%d]@%v\n", *modifiedTopicPartitions[index].Topic, modifiedTopicPartitions[index].Partition, modifiedTopicPartitions[index].Offset)
	}

	// query the earliest offset for each partition to reset to
	resultTopicPartitions, err := consumer.OffsetsForTimes(modifiedTopicPartitions, 10000)
	if err != nil {
		fmt.Printf("Timestamp offset search error: %v\n", err)
		return err
	}

	// assign the earliest offsets available, check assignment went well
	err = consumer.Assign(resultTopicPartitions)
	fmt.Println("Partition re-assignment")
	for _, partition := range resultTopicPartitions {
		fmt.Printf("resetTopicPartitionsOffsets() re-assigned offsets: %s[%d]@%v\n", *partition.Topic, partition.Partition, partition.Offset)
	}
	if err != nil {
		fmt.Printf("Partition assignment error: %v\n", err)
		return err
	}

	return nil
}

// Implementation of Kafka consumer polling operation via a high-level ReadMessage() function.
// Apparently with this implementation we won't be able to Seek/Assign the offset.
func (kafkaStore KafkaStore) readMessage(consumer *kafka.Consumer, writer *io.PipeWriter) error {
	// reset topic partitions offsets to the earliest first
	err := kafkaStore.resetTopicPartitionsOffsets(consumer)
	if err != nil {
		return err
	}

	// iteratively feed messages from kafka topic into the pipe
	for {
		fmt.Println("Reading a message from kafka")
		msg, err := consumer.ReadMessage(-1) // use -1 to eliminate timeout and wait indefinitely or time.Second * 10 for timeout of 10 seconds
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
			fmt.Printf("Reached highWatermark, closing the writer.\n")
			writer.Close()
			return nil
		}
	}
}

func (kafkaStore KafkaStore) ReplaceMeta(ctx context.Context, callback func(writer io.Writer) error) error {
	return errors.New("not implemented")
}

func (kafkaStore KafkaStore) AppendMeta(ctx context.Context, callback func(writer io.Writer) error) error {
	reader, writer := io.Pipe()

	// create a producer for Kafka
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaStore.BootstrapServers})
	if err != nil {
		fmt.Printf("Error creating consumer: %s", err)
		return err
	}
	defer producer.Close()
	fmt.Println("Initialized new producer")

	// call the callback
	producerErrors := make(chan error, 0)
	producerCallback := func(producerErrors chan error) {
		err = callback(writer)
		if err != nil {
			fmt.Printf("Error in callback: %s", err)
			producerErrors <- err
		} else {
			producerErrors <- nil
		}
	}
	go producerCallback(producerErrors)

	buffer := make([]byte, 1000000)
	// TODO: handle buffer overflow!!!!!!!!!!!!
	fmt.Println("Preparing to Read()")
	bytesRead, err := reader.Read(buffer)
	fmt.Println("Done reading")

	// handle EOF or error in the reader
	switch err {
	case io.EOF:
		break
	case nil:
		break
	default:
		fmt.Printf("AppendMeta(): failed to read message: %s\n", err)
		return err
	}
	fmt.Println("Done switching err after reading")

	err = <-producerErrors
	if err != nil {
		fmt.Printf("AppendMeta(): Failed to receive message from callback: %s \n", err)
		return nil
	}
	fmt.Println("Done checking producer err")

	// Optional delivery channel, if not specified the Producer object's
	// .Events channel is used.
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	fmt.Println("Preparing to Produce() message")
	// send payload to Kafka
	payload := buffer[0:bytesRead]
	fmt.Printf("AppendMeta() payload = %s\n", payload)
	message := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kafkaStore.Topic, Partition: kafka.PartitionAny},
		Value:          []byte(payload),
	}
	producer.Produce(&message, deliveryChan)
	fmt.Println("Done Producing message")
	producer.Flush(15 * 1000)
	fmt.Println("Done Flushing message")

	// check if the delivery succeeded
	event := <-deliveryChan
	fmt.Println("Retrieved an event from Kafka")
	switch message := event.(type) {
	case *kafka.Message:
		if message.TopicPartition.Error != nil {
			fmt.Printf("AppendMeta(): delivery to Kafka failed: %v\n", message.TopicPartition.Error)
			return message.TopicPartition.Error
		} else {
			fmt.Printf("AppendMeta(): delivered message to topic %s [%d] at offset %v\n",
				*message.TopicPartition.Topic, message.TopicPartition.Partition, message.TopicPartition.Offset)
		}
	default:
		fmt.Printf("Producer event is not a Kafka message")
	}
	fmt.Println("Done retrieving event from Kafka")

	return nil
}
