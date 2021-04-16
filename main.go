package main

import (
	"context"
	"fmt"
	"io"
)

func main() {
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
}
