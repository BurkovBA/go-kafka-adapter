Installation instructions
=========================

Mac (Arm64 architecture)
------------------------

To install "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka" on Mac Apple M1, say:

- `brew install librdkafka`
- `go get -d github.com/confluentinc/confluent-kafka-go/kafka`
- `export LDFLAGS="-L$(brew --prefix)/opt/librdkafka/lib -L$(brew --prefix)/opt/openssl/lib"`
- `export CPPFLAGS=-I$(brew --prefix)/opt/librdkafka/include/librdkafka`
- `export PKG_CONFIG_PATH="$(brew --prefix)/opt/openssl/lib/pkgconfig"`

Build command
-------------
`go build -tags dynamic main.go kafka_store.go`

Test command
------------
`go test -tags dynamic kafka_store.go kafka_store_test.go`
`ok      command-line-arguments  17.199s`

Kafka management commands
-------------------------

To deploy containerized kafka and zookeeper testing environment, follow the instructions:

https://kafka-tutorials.confluent.io/message-ordering/kafka.html#initialize-the-project

To learn how to use console producer and consumer, see:

https://kafka.apache.org/quickstart

Enter the kafka container with `docker exec -it broker /bin/bash`. Then in its 
shell you can run some monitoring and management commands.

### List topics
`kafka-topics --list --zookeeper zookeeper:2181`

### Describe partitions and replicas of a specific topic
`kafka-topics --bootstrap-server localhost:9092 --topic myTopic --describe`
