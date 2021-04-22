package main

import (
	"context"
	"io"
	"testing"
)

func TestLoadMeta(t *testing.T) {
	var ctx context.Context = context.Background()
	var kafkaStore *KafkaStore = NewKafkaStore("localhost:29092", "myTopic", "goConsumerGroup")

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

func TestAppendMeta(t *testing.T) {
	var ctx context.Context = context.Background()
	var kafkaStore *KafkaStore = NewKafkaStore("localhost:29092", "myTopic", "")

	var callback = func(writer io.Writer) error {
		for {

		}
	}

	err := kafkaStore.AppendMeta(ctx, callback)
	if err != nil {
		t.Errorf("TestAppendMeta() error: %s\n", err)
	}
}

func TestReplaceMeta(t *testing.T) {
	t.Skip("not implemented")
}
