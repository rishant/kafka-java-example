package com.example.native_kafka_poc.producer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaProducer5SyncAndAsyncProducerJsonStringValueExample {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String PRODUCER_NAME = "KafkaProducerTopicJsonStringExample2-" + UUID.randomUUID();
    private static final String TOPIC_NAME = "execution-data";
    // ObjectMapper for JSON serialization
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        // Configure Kafka producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_NAME);
        // Serialization settings
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Optimizations
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip"); // Compress messages to reduce network usage
        props.put(ProducerConfig.ACKS_CONFIG,  "all"); // Wait for all replicas to acknowledge
        // Batch Processing Properties
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // Batch size in bytes (16 KB)
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10); // Wait up to 10ms before sending a batch
        // Failure Usecase
        props.put(ProducerConfig.RETRIES_CONFIG, 3); // Retry up to 3 times
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);  // 100ms backoff between retries

        // Create a Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);
        // Produce messages synchronously
        produceSynchronously(producer, PRODUCER_NAME, TOPIC_NAME);
        // Produce messages asynchronously
        produceAsynchronously(producer, PRODUCER_NAME, TOPIC_NAME);
        // Close the producer
        producer.close();
        System.out.printf("Producer [%s]: Closed.%n", PRODUCER_NAME);        
    }
    
    /**
     * Produces messages synchronously.
     */
    private static void produceSynchronously(Producer<String, String> producer, String producerName, String topicName) {
        System.out.println("Producing messages synchronously...");
        try {
            for (int i = 1; i <= 5; i++) {
                String key = "SyncKey" + i;

                // Create a sample payload
				Map<String, Object> payload = new HashMap<>();
				payload.put("id", i);
				payload.put("message", "Optimized Message " + i);
				payload.put("timestamp", System.currentTimeMillis());
				// Convert payload to JSON string
				String jsonValue = objectMapper.writeValueAsString(payload);

                // Create a ProducerRecord
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, jsonValue);

                // Send the record synchronously
                Future<RecordMetadata> future = producer.send(record);
                RecordMetadata metadata = future.get(); // Blocks until the record is acknowledged

                // Log the result
                System.out.printf("Sync:Producer [%s]: Sent message with key=%s to topic=%s, partition=%d, offset=%d%n",
                		producerName, key, metadata.topic(), metadata.partition(), metadata.offset());
            }
        } catch (JsonProcessingException e) {
            System.err.printf("Async:Producer [%s]: Error during sending: %s%n", producerName, e.getMessage());
		} catch (Exception e) {
            System.err.printf("Sync:Producer [%s]: Error during sending: %s%n", producerName, e.getMessage());
        }
    }

    /**
     * Produces messages asynchronously.
     */
    private static void produceAsynchronously(Producer<String, String> producer, String producerName, String topicName) {
        System.out.println("Producing messages asynchronously...");
        for (int i = 1; i <= 5; i++) {

			try {
				String key = "AsyncKey" + i;
				
				// Create a sample payload
				Map<String, Object> payload = new HashMap<>();
				payload.put("id", i);
				payload.put("message", "Optimized Message " + i);
				payload.put("timestamp", System.currentTimeMillis());
				// Convert payload to JSON string
				String jsonValue = objectMapper.writeValueAsString(payload);
				
				// Create a ProducerRecord
				ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, jsonValue);
				
				// Send the record asynchronously
				producer.send(record, (RecordMetadata metadata, Exception exception) -> {
					if (exception == null) {
						// Log the result
						System.out.printf("Async:Producer [%s]: Sent message with key=%s to topic=%s, partition=%d, offset=%d%n",
								producerName, key, metadata.topic(), metadata.partition(), metadata.offset());
					} else {
						// Log the error
						System.err.printf("Async:Producer [%s]: Error sending message with key=%s: %s%n", producerName, key, exception.getMessage());
					}
				});
			} catch (JsonProcessingException e) {
	            System.err.printf("Async:Producer [%s]: Error during sending: %s%n", producerName, e.getMessage());
			}
        }
    }
}
