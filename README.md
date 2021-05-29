Installation instructions
=========================

Build command
-------------
`go build main.go kafka_store.go`

Test command
------------
`go test kafka_store.go kafka_store_test.go`

`ok      command-line-arguments  17.199s`

Kafka management commands
-------------------------

To deploy containerized kafka and zookeeper testing environment, follow the instructions:

https://kafka-tutorials.confluent.io/message-ordering/kafka.html#initialize-the-project

To learn how to use console producer and consumer, see:

https://kafka.apache.org/quickstart

Enter the kafka container with `docker exec -it broker /bin/bash`. Then in its 
shell you can run some monitoring and management commands.

### Create a topic
`kafka-topics --create --topic myTopic --bootstrap-server localhost:9092`

### List topics
`kafka-topics --list --zookeeper zookeeper:2181`

### Describe partitions and replicas of a specific topic
`kafka-topics --bootstrap-server localhost:9092 --topic myTopic --describe`
