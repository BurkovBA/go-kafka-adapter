package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/Shopify/sarama"
)

// KafkaStoreConsumerGroupHandler is the set of actual callbacks invoked as the lifecycle hooks of a consumer group.
// It is passed on input of sarama.ConsumerGroup.Consume() method and runs the actual business logic.
// See the docs of ConsumerGroup.Consume(): https://github.com/Shopify/sarama/blob/master/consumer_group.go#L18
type KafkaStoreConsumerGroupHandler struct {
	writer   *io.PipeWriter
	reader   *io.PipeReader
	callback func(reader io.Reader) error
	status   chan error
	client   sarama.Client
}

func (handler *KafkaStoreConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (handler *KafkaStoreConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

// Check, if the consumerGroup is currently at highWatermark
func (handler *KafkaStoreConsumerGroupHandler) isAtHighwatermark(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim, offset int64) (bool, error) {
	// check, if we're already reached the highWatermark
	topic := claim.Topic()
	partition := claim.Partition()

	highWatermark, err := handler.client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		fmt.Printf("Failed to read high watermark: %s\n", err)
		return false, err
	}

	fmt.Printf("highWatermark = %d, offset = %d\n", highWatermark, offset)

	// if we've reached highWatermark or there are no messages in kafka at all, return
	if offset >= highWatermark || highWatermark == 0 {
		return true, nil
	} else {
		return false, nil
	}
}

// This function is called within a goroutine, so no need to wrap it.
// Callgraph is:
// ConsumerGroup.Consume() -> ConsumerGroup.newSession() -> newConsumerGroupSession() -> ConsumerGroupSession.consume() -> ConsumerGroupHandler.ConsumeClaims()
func (handler *KafkaStoreConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Println("Called ConsumeClaim()")

	// initialze the callback goroutine
	go func() {
		err := handler.callback(handler.reader)
		if err != nil {
			return
		}
	}()

	defer func() {
		// close the reader, otherwise may be blocked.
		handler.reader.Close()
	}()

	isAtHighWatermark, err := handler.isAtHighwatermark(session, claim, claim.InitialOffset())
	if err != nil {
		return err
	}
	if isAtHighWatermark {
		fmt.Println("Already at highWatermark")
		return nil
	}

	// loop through the messages
	fmt.Println("Starting to loop through the messages")
	for message := range claim.Messages() {
		fmt.Printf("ConsumeClaim() processing message: '%s', offset: %d\n", message.Value, message.Offset)

		// write the message into callback through the pipe
		_, err := handler.writer.Write(message.Value)
		if err != nil {
			return err
		}

		// mark the message consumer so that it will be autocommited after KafkaStoreConsumerGroupHandler.Cleanup()
		session.MarkMessage(message, "")

		// if we've reached highWatermark, return; note that here we add 1 to message.Offset to compare it with highWatermark
		isAtHighWatermark, err = handler.isAtHighwatermark(session, claim, message.Offset+1)
		if err != nil {
			return err
		}

		if isAtHighWatermark {
			fmt.Printf("message.Offset+1 (%d) >= highWatermark\n", message.Offset+1)
			return nil
		}
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
	BootstrapServer string // kafka brokers to negotiate the protocol with upon consumer bootstrap, default: "localhost:9092"
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
		status:   make(chan error),
		writer:   writer,
		reader:   reader,
		callback: callback,
		client:   client,
	}

	// start consuming
	go func() {
		// infinite loop is required in case of rebalancing, see:
		// https://github.com/Shopify/sarama/blob/master/examples/consumergroup/main.go
		for {
			// no need to create a goroutine here, it is created automatically
			fmt.Println("Preparing to start consumerGroup.Consume()")
			err := consumerGroup.Consume(ctx, []string{kafkaStore.Topic}, &handler)

			// TODO: handle errors
			if err != nil {
				handler.status <- err
				return
			}

			if ctx.Err() != nil {
				handler.status <- ctx.Err()
				return
			}

			// We've reached high watermark, finish processing
			if err == nil {
				fmt.Println("Finish processing, we've reached the highWatermark")
				handler.status <- nil
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		fmt.Println("Context cancelled LoadMeta() execution")
		return nil
	case err := <-handler.status:
		if err != nil {
			fmt.Printf("Got an error, while running Consume(): %s \n", err)
		} else {
			fmt.Println("Successfully exited from Consume()")
		}
		return err
	}

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

		// otherwise will be blocked.
		writer.Close()

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
		fmt.Println("Calling Read()")
		bytesRead, err := reader.Read(buffer)
		if err == io.EOF {
			fmt.Println("Received EOF in Read()")
			payload = append(payload, buffer[:bytesRead]...)
			break
		} else if err != nil {
			fmt.Printf("AppendMeta(): failed to read message: %s\n", err)
			return err
		} else {
			fmt.Println("Read() without exceptions")
			payload = append(payload, buffer[:bytesRead]...)
		}
	}
	fmt.Println("Done reading")
	fmt.Printf("AppendMeta() payload = '%s'\n", payload)

	// check for read errors
	err = <-producerErrors
	if err != nil {
		fmt.Printf("AppendMeta(): Failed to receive message from callback: %s \n", err)
		return nil
	}
	fmt.Println("Done checking producer err")

	// send payload to Kafka
	fmt.Printf("Preparing to Produce() message: '%s'\n", payload)
	message := &sarama.ProducerMessage{
		Topic:     kafkaStore.Topic,
		Partition: -1,
		Value:     sarama.StringEncoder(payload),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		fmt.Printf("Failed to deliver message %s to partition %d at offset %d: %s\n", payload, partition, offset, err)
		return err
	}
	fmt.Printf("Done Produce() message: '%s'\n", payload)

	return nil
}
