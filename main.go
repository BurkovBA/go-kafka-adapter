package main

import (
	"context"
	"fmt"
	"io"
	// _ "net/http/pprof"
)

func TestConsumer() {
	var ctx context.Context = context.Background()

	var callback = func(reader io.Reader) error {
		fmt.Println("Started callback")

		buffer := make([]byte, 1000)
		for {
			fmt.Println("Preparing to block in reader.Read()")
			bytesRead, err := reader.Read(buffer)
			fmt.Println("Successfully exiting reader.Read()")
			buffer = buffer[0 : bytesRead+1]
			fmt.Println("Output of the Reader:")
			fmt.Println(buffer)

			if err != nil {
				return err
			}
		}

		return nil
	}

	var kafkaStore *KafkaStore = NewKafkaStore("localhost:29092", "myTopic", "goConsumerGroup")
	kafkaStore.LoadMeta(ctx, callback)
	return
}

func TestProducer() {
	var ctx context.Context = context.Background()

	var callback = func(writer io.Writer) error {
		fmt.Println("Called callback()")
		msg := []byte("This is a programmatically produced message 1")
		_, err := writer.Write(msg)
		if err != nil {
			fmt.Printf("Error encountered while Writing: %s \n", err)
			return err
		}
		return nil
	}

	fmt.Println("Calling NewKafkaStore()")
	var kafkaStore *KafkaStore = NewKafkaStore("localhost:29092", "", "")
	err := kafkaStore.AppendMeta(ctx, callback)
	if err != nil {
		fmt.Printf("TestProducer() error: %s\n", err)
	}
	return
}

func main() {
	// go func() {
	// log.Println(http.ListenAndServe("localhost:6060", nil))
	// }()
	TestProducer()
}
