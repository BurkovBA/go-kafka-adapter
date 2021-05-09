package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os/exec"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type KafkaAdapterTestSuite struct {
	suite.Suite
	Topic           string
	BootstrapServer string
	Zookeeper       string
	ConsumerGroup   string
}

func (suite *KafkaAdapterTestSuite) SetupSuite() {
	suite.Topic = strconv.Itoa(rand.Intn(10000))
	suite.BootstrapServer = "localhost:29092"
	suite.Zookeeper = "zookeeper:2181"
	suite.ConsumerGroup = "goConsumerGroup"

	// initialize topic
	cmd := exec.Command("docker", "exec", "broker", "kafka-topics", "--create", "--topic", suite.Topic, "--bootstrap-server", suite.BootstrapServer, "--replication-factor", "1", "--partitions", "1")
	err := cmd.Run()
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
}

func (suite *KafkaAdapterTestSuite) TearDownSuite() {
	// destroy Topic
	// cmd := exec.Command("docker", "exec", "broker", "kafka-topics", "--zookeeper", suite.Zookeeper, "--delete", "--topic", suite.Topic)
	// err := cmd.Run()
	// if err != nil {
	// panic(err)
	// }
}

func (suite *KafkaAdapterTestSuite) TestLoadMeta() {
	var ctx context.Context = context.Background()
	var kafkaStore *KafkaStore = NewKafkaStore(suite.BootstrapServer, suite.Topic, suite.ConsumerGroup)

	// produce a message 'foobar' using kafka console client
	command := fmt.Sprintf("bash \"foobar\" | docker exec broker kafka-console-producer --bootstrap-server %s --topic %s", suite.BootstrapServer, suite.Topic)
	cmd := exec.Command("bach", "-c", command)
	err := cmd.Run()
	if err != nil {
		panic(err)
	}

	data := make([]byte, 1000000)
	var callback = func(reader io.Reader) error {
		buffer := make([]byte, 1000000)
		for {
			bytesRead, err := reader.Read(buffer)
			if err == io.EOF {
				break
				data = append(data, buffer[:bytesRead+1]...)
			} else if err != nil {
				return err
			} else {
				data = append(data, buffer[:bytesRead+1]...)
			}
		}
		return nil
	}

	err = kafkaStore.LoadMeta(ctx, callback)
	if err != nil {
		suite.T().Error("TestLoadMeta() error")
	}

	assert.Equal(suite.T(), "foobar", string(data), "Received message should be the same as produced by console producer")
}

func (suite *KafkaAdapterTestSuite) TestAppendMeta() {
	var ctx context.Context = context.Background()
	var kafkaStore *KafkaStore = NewKafkaStore(suite.BootstrapServer, suite.Topic, "")

	var callback = func(writer io.Writer) error {
		msg := []byte("This is a programmatically produced message")
		_, err := writer.Write(msg)
		if err != nil {
			return err
		}
		return nil
	}

	err := kafkaStore.AppendMeta(ctx, callback)
	if err != nil {
		suite.T().Error("TestAppendMeta() error")
		// t.Errorf("TestAppendMeta() error: %s\n", err)
	}

	// consume the message with console consumer, make sure that it is the same as we passed on input
	cmd := exec.Command("docker", "exec", "broker", "kafka-console-consumer", "--from-beginning", "--bootstrap-server", suite.BootstrapServer, "--topic", suite.Topic, "--timeout-ms", "15000")
	output, err := cmd.Output()
	if err != nil {
		panic(err)
	}
	fmt.Printf("TestAppendMeta(): cmd.Output = %s", output)
	assert.Equal(suite.T(), "This is a programmatically produced message", string(output), "Console consumer should receive the produced message")
}

func (suite *KafkaAdapterTestSuite) TestReplaceMeta() {
	suite.T().Skip("not implemented")
}

func TestKafkaAdapterTestSuite(t *testing.T) {
	suite.Run(t, new(KafkaAdapterTestSuite))
}
