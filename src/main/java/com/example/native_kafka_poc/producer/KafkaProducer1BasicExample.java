package com.example.native_kafka_poc.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.UUID;

public class KafkaProducer1BasicExample {
    public static void main(String[] args) {
        // Kafka configuration properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Replace with your Kafka broker address
        String producerName = "KafkaProducerBasicExample-" + UUID.randomUUID(); // Producer unique identifier
        props.put("client.id", producerName);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("acks", "all"); // Wait for all replicas to acknowledge
//        props.put("retries", 3); // Retry up to 3 times
//        props.put("batch.size", 16384); // Batch size in bytes (16 KB)
//        props.put("linger.ms", 10); // Wait up to 10ms before sending a batch
//        props.put("compression.type", "gzip"); // Compress messages to reduce network usage

        // Create a Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Topic name
        String topicName = "event_end";

        try {
            for (int i = 1; i <= 10; i++) {
                // Create a record to send
                String key = "Key" + i;
                String value = "Message " + i;

                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

                // Send the record asynchronously and handle the result
                producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                    if (exception == null) {
                        System.out.printf("Producer [%s]: Sent message with key=%s to topic=%s, partition=%d, offset=%d%n",
                                producerName, key, metadata.topic(), metadata.partition(), metadata.offset());
                    } else {
                        System.err.printf("Producer [%s]: Error sending message with key=%s: %s%n",
                                producerName, key, exception.getMessage());
                    }
                });
            }
        } finally {
            // Close the producer to release resources
            producer.close();
            System.out.printf("Producer [%s]: Closed.%n", producerName);
        }
    }
}
