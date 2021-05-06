package main

import (
	"context"
	"io"
	"math/rand"
	"os/exec"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
)

type KafkaAdapterTestSuite struct {
	suite.Suite
	Topic           string
	BootstrapServer string
	Zookeeper       string
	ConsumerGroup   string
}

func (suite *KafkaAdapterTestSuite) SetupTest() {
	suite.Topic = strconv.Itoa(rand.Intn(10000))
	suite.Zookeeper = "localhost:2181"
	suite.ConsumerGroup = "goConsumerGroup"
	suite.BootstrapServer = "localhost:29092"

	// initialize topic
	cmd := exec.Command("kafka-topics.sh", "--create", "--topic", suite.Topic, "--bootstrap-server", suite.BootstrapServer)
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
}

func (suite *KafkaAdapterTestSuite) TeadDown() {
	// destroy Topic
	cmd := exec.Command("kafka-topics.sh", "--zookeeper", suite.Zookeeper, "--delete", "--topic", suite.Topic)
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
}

func (suite *KafkaAdapterTestSuite) TestLoadMeta(t *testing.T) {
	var ctx context.Context = context.Background()
	var kafkaStore *KafkaStore = NewKafkaStore(suite.BootstrapServer, suite.Topic, suite.ConsumerGroup)

	var callback = func(reader io.Reader) error {
		for {
			buffer := make([]byte, 1000000)
			bytesRead, err := reader.Read(buffer)
			buffer = buffer[0 : bytesRead+1]

			if err != nil {
				return err
			}
		}
	}

	err := kafkaStore.LoadMeta(ctx, callback)
	if err != nil {
		t.Errorf("TestLoadMeta() error: %s\n", err)
	}
}

func (suite *KafkaAdapterTestSuite) TestAppendMeta(t *testing.T) {
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
		t.Errorf("TestAppendMeta() error: %s\n", err)
	}
}

func TestReplaceMeta(t *testing.T) {
	t.Skip("not implemented")
}
