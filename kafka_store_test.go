package main

import (
	"context"
	"fmt"
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

func (suite *KafkaAdapterTestSuite) SetupSuite() {
	suite.Topic = strconv.Itoa(rand.Intn(10000))
	suite.BootstrapServer = "localhost:29092"
	suite.Zookeeper = "zookeeper:2181"
	suite.ConsumerGroup = "goConsumerGroup"

	fmt.Println("SetupTest()")

	// initialize topic
	cmd := exec.Command("docker", "exec", "broker", "kafka-topics", "--create", "--topic", suite.Topic, "--bootstrap-server", suite.BootstrapServer, "--replication-factor", "1", "--partitions", "1")
	err := cmd.Run()
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
}

func (suite *KafkaAdapterTestSuite) TearDownSuite() {
	fmt.Println("TearDown()")

	// destroy Topic
	cmd := exec.Command("docker", "exec", "broker", "kafka-topics", "--zookeeper", suite.Zookeeper, "--delete", "--topic", suite.Topic)
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
}

func (suite *KafkaAdapterTestSuite) TestLoadMeta() {
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
		suite.T().Error("TestLoadMeta() error")
	}
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
}

func (suite *KafkaAdapterTestSuite) TestReplaceMeta() {
	suite.T().Skip("not implemented")
}

func TestKafkaAdapterTestSuite(t *testing.T) {
	suite.Run(t, new(KafkaAdapterTestSuite))
}
