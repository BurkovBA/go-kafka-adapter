package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/Shopify/sarama"
)

// KafkaStoreConsumerGroupHandler is the set of actual callbacks invoked as the lifecycle hooks of a consumer group.
// It is passed on input of sarama.ConsumerGroup.Consume() method and runs the actual business logic.
// See the docs of ConsumerGroup.Consume(): https://github.com/Shopify/sarama/blob/master/consumer_group.go#L18
type KafkaStoreConsumerGroupHandler struct {
	writer   *io.PipeWriter
	reader   *io.PipeReader
	callback func(reader io.Reader) error
	ready    chan bool
	client   sarama.Client
}

func (handler *KafkaStoreConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	close(handler.ready)
	return nil
}

func (handler *KafkaStoreConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

// This function is called within a goroutine, so no need to wrap it.
// Callgraph is:
// ConsumerGroup.Consume() -> ConsumerGroup.newSession() -> newConsumerGroupSession() -> ConsumerGroupSession.consume() -> ConsumerGroupHandler.ConsumeClaims()
func (handler *KafkaStoreConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// initialze the callback goroutine
	var waitgroup sync.WaitGroup
	waitgroup.Add(1)
	go func() {
		defer waitgroup.Done()
		err := handler.callback(handler.reader)
		if err != nil {
			return
		}
	}()
	waitgroup.Wait()

	// loop through the messages
	for message := range claim.Messages() {
		// break the loop if we've reached the highWatermark
		topic := claim.Topic()
		partition := claim.Partition()
		highWatermark, err := handler.client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("Failed to read highWatermark: %s", err)
			return err
		}

		if message.Offset+1 >= highWatermark {
			return nil
		}

		// write the message into callback through the pipe
		_, err = handler.writer.Write(message.Value)
		if err != nil {
			return err
		}

		// mark the message consumer so that it will be autocommited after KafkaStoreConsumerGroupHandler.Cleanup()
		session.MarkMessage(message, "")
	}

	return nil
}

// Kafka entities:
// - broker - single instance of Kakfa; you might have a cluster with e.g. 3 Kafka brokers on 3 separate servers for HA
// - topic - a single message queue, e.g. 'flats'
// - partition - you can shard topic by some criterion into partitions, e.g. split 'flats' into 'Moscow', 'St.Petersburg', 'Novosibirsk' partitions
// - replica - each partition can have a master and several replicas, each replica on ots broker; you can set up something like sync/async replication between them
// - producer - a process that writes messages into Kafka
// - offset - the incremental index of a message, unique within a topic partition for a consumer group
// - high watermark - the offset under which the next message, committed by the producer, will be committed
// - consumer - a process that reads messages from Kafka; it can listen to multiple topics and within a topic to several partitions
// - consumer group - a set of (possibly interchangeable) consumers, per which a unique offset is kept for each topic partition; if you're not planning to commit offsets, you can avoid using consumer groups
// - consumer group session - a period of time, throughout which the allocation of consumers to partitions stays the same
// - consumer group claim - within a consumer group session a claim registers ownership of a topic partition by a consumer
// - consumer group handler - within a consumer group session this is a set of lifecycle hooks that do consuming and setup/teardown

// See kafka architecture: https://www.oreilly.com/library/view/kafka-the-definitive/9781491936153/ch04.html
// See kafka protocol (important for understanding of Sarama implementation): https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
type KafkaStore struct {
	BootstrapServer string // kafka brokers to negotiate the protocol with upon consumer bootstrap, default: "localhost:29092"
	Topic           string // name of the topic that stores our data, default: "myTopic"
	ConsumerGroupId string // id of our consumer group, default: "goConsumerGroup"
}

func NewKafkaStore(BootstrapServer string, Topic string, ConsumerGroupId string) *KafkaStore {
	kafkaStore := &KafkaStore{}

	kafkaStore.BootstrapServer = BootstrapServer
	kafkaStore.Topic = Topic
	kafkaStore.ConsumerGroupId = ConsumerGroupId

	return kafkaStore
}

func (kafkaStore KafkaStore) LoadMeta(ctx context.Context, callback func(reader io.Reader) error) error {
	// writer side of the pipe is ours, reader is passed to the callback
	reader, writer := io.Pipe()

	// bootstrap the consumer
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	version, err := sarama.ParseKafkaVersion("2.3.0")
	if err != nil {
		return err
	}
	config.Version = version

	// initialize kafka client
	client, err := sarama.NewClient([]string{kafkaStore.BootstrapServer}, config)
	if err != nil {
		return err
	}
	defer client.Close()

	// initialize consumer group
	consumerGroup, err := sarama.NewConsumerGroupFromClient(kafkaStore.ConsumerGroupId, client)
	if err != nil {
		return err
	}
	defer consumerGroup.Close()
	fmt.Println("Created a new consumer group")

	// initialize a handler object to actually process consumerGroup messages
	handler := KafkaStoreConsumerGroupHandler{
		ready:    make(chan (bool)),
		writer:   writer,
		reader:   reader,
		callback: callback,
		client:   client,
	}

	// start consuming
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		for {
			// no need to create a goroutine here, it is created automatically
			err := consumerGroup.Consume(ctx, []string{kafkaStore.Topic}, &handler)
			if err != nil {
				return
			}

			if ctx.Err() != nil {
				return
			}

			handler.ready = make(chan bool)
		}
	}()
	waitGroup.Wait()

	// TODO: handle errors
	// TODO: handle context.Done()

	return nil
}

func (kafkaStore KafkaStore) ReplaceMeta(ctx context.Context, callback func(writer *io.PipeWriter) error) error {
	return errors.New("not implemented")
}

func (kafkaStore KafkaStore) AppendMeta(ctx context.Context, callback func(writer *io.PipeWriter) error) error {
	reader, writer := io.Pipe()

	// create a producer for Kafka
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{kafkaStore.BootstrapServer}, config)
	if err != nil {
		fmt.Printf("Error creating producer: %s", err)
		return err
	}
	defer producer.Close()
	fmt.Println("Initialized new producer")

	// start the callback in a goroutine
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

	// read a message from the callback
	fmt.Println("Preparing to Read()")
	payload := make([]byte, 0)
	buffer := make([]byte, 1000000)
	for {
		bytesRead, err := reader.Read(buffer)
		if err == io.EOF {
			break
			payload = append(payload, buffer[:bytesRead]...)
		} else if err != nil {
			fmt.Printf("AppendMeta(): failed to read message: %s\n", err)
			return err
		} else {
			payload = append(payload, buffer[:bytesRead]...)
		}
	}
	fmt.Println("Done reading")
	fmt.Printf("AppendMeta() payload = %s\n", payload)

	// check for read errors
	err = <-producerErrors
	if err != nil {
		fmt.Printf("AppendMeta(): Failed to receive message from callback: %s \n", err)
		return nil
	}
	fmt.Println("Done checking producer err")

	// send payload to Kafka
	fmt.Println("Preparing to Produce() message")
	message := &sarama.ProducerMessage{
		Topic:     kafkaStore.Topic,
		Partition: -1,
		Value:     sarama.StringEncoder(payload),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		fmt.Printf("Failed to deliver message %s to partition %s at offset &s: ", payload, partition, offset, err)
		return err
	}

	return nil
}
