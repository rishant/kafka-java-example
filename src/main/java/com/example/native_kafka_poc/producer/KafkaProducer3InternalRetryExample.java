package com.example.native_kafka_poc.producer;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducer3InternalRetryExample {

	public static void main(String[] args) {
        // Kafka configuration properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Replace with your Kafka broker address
        
        String producerName = "KafkaProducerInternalRetryExample-" + UUID.randomUUID(); // Producer unique identifier
        props.put("client.id", producerName);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Configure retries
        props.put(ProducerConfig.ACKS_CONFIG, "all");              // Wait for all replicas to acknowledge
        props.put(ProducerConfig.RETRIES_CONFIG, 5);                // Max retry attempts
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 200);    // 200ms Backoff time between retries
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000); // Total time for retries before failure
        
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
