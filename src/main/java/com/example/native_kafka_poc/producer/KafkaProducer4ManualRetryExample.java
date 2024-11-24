package com.example.native_kafka_poc.producer;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducer4ManualRetryExample {

	public static void main(String[] args) throws InterruptedException {
		// Kafka configuration properties
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092"); // Replace with your Kafka broker address

		String producerName = "KafkaProducerExternalRetryExample-" + UUID.randomUUID(); // Producer unique identifier
		props.put("client.id", producerName);

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		props.put("acks", "all"); // Wait for all replicas to acknowledge
		props.put(ProducerConfig.RETRIES_CONFIG, 0); // Disable Kafka's built-in retries
		props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100); // Backoff between retries

		Producer<String, String> producer = new KafkaProducer<>(props);

		// Step 2: Iterate from 1 to 10 and send messages
		for (int i = 1; i <= 10; i++) {
			String key = "Key-" + i;
			String value = "Message-" + i;

			ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", key, value);

			// Step 3: Retry Mechanism
			int maxRetries = 3;
			boolean success = false;

			for (int retryCount = 0; retryCount < maxRetries; retryCount++) {
				try {
					// Synchronously send the message and wait for acknowledgment
					producer.send(record).get(); // Blocks until the message is acknowledged
					System.out.printf("Message sent successfully: Key=%s, Value=%s%n", key, value);
					success = true; // Mark success if no exception occurs
					break; // Exit retry loop if successful
				} catch (Exception e) {
					System.err.printf("Attempt %d failed for Key=%s, Value=%s. Error: %s%n", retryCount + 1, key, value,
							e.getMessage());
					// Wait before retrying (manual backoff)
					try {
						Thread.sleep(100);
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
						System.err.println("Retry sleep interrupted");
					}
				}
			}

			if (!success) {
				System.err.printf("Message failed after %d attempts: Key=%s, Value=%s%n", maxRetries, key, value);
				// Optionally, you can send the message to a Dead Letter Queue (DLQ) here
			}
		}

		// Step 4: Close the producer
		producer.close();
		System.out.println("Producer closed.");
	}
}
