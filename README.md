# Demo
https://kafka.apache.org/23/documentation/streams/quickstart

# Start
Commands to start:
```
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

# Create topics
Create the input topic:
```
bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic streams-plaintext-input
```

Output topic, this is compacted so that it only holds the most recent message per key https://kafka.apache.org/23/documentation/streams/quickstart#anchor-changelog-output
```
bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic streams-wordcount-output \
    --config cleanup.policy=compact
```

Can describe the topics:
```
bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
```

# Run application
To run the application can call:
```
bin/kafka-run-class.sh org.apache.kafka.streams.examples.wordcount.WordCountDemo
```

# Read output
To read the output of the stream can run:
```
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic streams-wordcount-output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

# Write input
Producer to input text:
```
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic streams-plaintext-input
```

# Delete Topics
```
bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic streams-plaintext-input
bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic streams-wordcount-output
```
