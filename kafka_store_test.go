package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os/exec"
	"strconv"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type KafkaAdapterTestSuite struct {
	suite.Suite
	BootstrapServer string
	Zookeeper       string
	ConsumerGroup   string
}

func (suite *KafkaAdapterTestSuite) SetupSuite() {
	suite.BootstrapServer = "localhost:29092"
	suite.Zookeeper = "zookeeper:2181"
	suite.ConsumerGroup = "goConsumerGroup"

	// start kafka broker and zookeeper in docker-compose
	cmd := exec.Command("docker-compose", "up", "-d")
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
}

// initialize topic with a randomly selected name, 1 partition and 1 replica;
// return string topic name and/or error
func (suite *KafkaAdapterTestSuite) CreateRandomTopic() (string, error) {
	topic := strconv.Itoa(rand.Intn(10000))
	cmd := exec.Command("docker", "exec", "broker", "kafka-topics", "--create", "--topic", topic, "--bootstrap-server", suite.BootstrapServer, "--replication-factor", "1", "--partitions", "1")
	err := cmd.Run()
	if err != nil {
		fmt.Printf("CreateRandomTopic() resulted in an error\n")
		fmt.Println(err)
		return "", err
	} else {
		return topic, nil
	}
}

// destroy Topic of a specific name
func (suite *KafkaAdapterTestSuite) DestroyTopic(topic string) error {
	cmd := exec.Command("docker", "exec", "broker", "kafka-topics", "--zookeeper", suite.Zookeeper, "--delete", "--topic", topic)
	err := cmd.Run()
	if err != nil {
		return err
	} else {
		return nil
	}
}

// produce messages from a json file using kafka console producer
func (suite *KafkaAdapterTestSuite) ProduceWithConsoleProducer(topic string, file string) error {
	cmd := exec.Command("docker", "exec", "broker", "kafka-console-producer", "--bootstrap-server", suite.BootstrapServer, "--topic", topic, "<", file)
	err := cmd.Run()
	if err != nil {
		return err
	}
	return nil
}

func (suite *KafkaAdapterTestSuite) TearDownSuite() {
	// stop docker-compose and destroy volumes
	cmd := exec.Command("docker-compose", "down", "-v")
	err := cmd.Run()
	if err != nil {
		panic(err)
	}

}

func (suite *KafkaAdapterTestSuite) TestLoadMeta() {
	var ctx context.Context = context.Background()
	topic, err := suite.CreateRandomTopic()
	if err != nil {
		panic(err)
	}
	var kafkaStore *KafkaStore = NewKafkaStore(suite.BootstrapServer, topic, suite.ConsumerGroup)

	// produce a json message using kafka console producer
	// err = suite.ProduceWithConsoleProducer(topic, "test.json")
	// if err != nil {
	// panic(err)
	// }

	// produce a message programmatically (console producer is meant to be used interactively and is glitchy)
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaStore.BootstrapServers})
	if err != nil {
		fmt.Printf("Error creating producer: %s", err)
		panic(err)
	}
	defer producer.Close()

	message := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kafkaStore.Topic, Partition: kafka.PartitionAny},
		Value:          []byte("foobar"),
	}
	producer.Produce(&message, nil)
	producer.Flush(15 * 1000)

	// prepare a callback for consumer
	data := make([]byte, 0)
	var callback = func(reader io.Reader) error {
		buffer := make([]byte, 1000000)
		for {
			bytesRead, err := reader.Read(buffer)
			if err == io.EOF {
				break
				data = append(data, buffer[:bytesRead]...)
			} else if err != nil {
				return err
			} else {
				data = append(data, buffer[:bytesRead]...)
			}
		}
		return nil
	}

	// run LoadMeta, check correctness of the received message
	err = kafkaStore.LoadMeta(ctx, callback)
	if err != nil {
		suite.T().Error("TestLoadMeta() error")
	}

	assert.Equal(suite.T(), "foobar", string(data), "Received message should be the same as produced by console producer")
}

func (suite *KafkaAdapterTestSuite) TestAppendMeta() {
	var ctx context.Context = context.Background()
	topic, err := suite.CreateRandomTopic()
	if err != nil {
		panic(err)
	}
	var kafkaStore *KafkaStore = NewKafkaStore(suite.BootstrapServer, topic, "")

	var callback = func(writer *io.PipeWriter) error {
		msg := []byte("This is a programmatically produced message")
		_, err := writer.Write(msg)
		if err != nil {
			writer.Close()
			return err
		}
		writer.Close()
		return nil
	}

	err = kafkaStore.AppendMeta(ctx, callback)
	if err != nil {
		suite.T().Error("TestAppendMeta() error")
		// t.Errorf("TestAppendMeta() error: %s\n", err)
	}

	// consume the message with console consumer, make sure that it is the same as we passed on input
	cmd := exec.Command("docker", "exec", "broker", "kafka-console-consumer", "--from-beginning", "--bootstrap-server", suite.BootstrapServer, "--topic", topic, "--timeout-ms", "15000")
	output, err := cmd.Output()
	if err != nil {
		panic(err)
	}
	fmt.Printf("TestAppendMeta(): cmd.Output = %s", output)
	assert.Equal(suite.T(), "This is a programmatically produced message\n", string(output), "Console consumer should receive the produced message")
}

func (suite *KafkaAdapterTestSuite) TestReplaceMeta() {
	suite.T().Skip("not implemented")
}

func TestKafkaAdapterTestSuite(t *testing.T) {
	suite.Run(t, new(KafkaAdapterTestSuite))
}
