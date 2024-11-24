package com.example.native_kafka_poc.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

public class KafkaConsumer4SubscribeTopicSyncAndAsyncExample {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CONSUMER_GROUP_NAME = "consumer_group_sync_async_execution_data_1";
    private static final String CONSUMER_NAME = "KafkaConsumerSubscribeTopicSyncAndAsyncExample4-" + UUID.randomUUID();
    private static final String ENABLE_AUTO_COMMIT = "false";
    private static final String MAX_POLL_RECORDS = "1";
    private static final String TOPIC_NAME = "execution-data";
    
    public static void main(String[] args) {
    	// Configure Kafka consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME); // Set a unique consumer-group name
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, CONSUMER_NAME); // Set a unique consumer name
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT); // Disable auto-commit for sync and async commits
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Developer testing

        // Create Kafka consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        System.out.printf("Consumer [%s]: Subscribed to topic [%s].%n", CONSUMER_NAME, TOPIC_NAME);

        // Consume messages synchronously
        consumeSynchronously(consumer, CONSUMER_NAME);

        // Consume messages asynchronously
        consumeAsynchronously(consumer, CONSUMER_NAME);

        // Close the consumer
        consumer.close();
        System.out.printf("Consumer [%s]: Closed.%n", CONSUMER_NAME);
    }

    /**
     * Consumes messages synchronously with manual commit.
     */
    private static void consumeSynchronously(Consumer<String, String> consumer, String consumerName) {
        System.out.printf("Consumer [%s]: Consuming messages synchronously...%n", consumerName);
        try {
            while (true) {
                // Poll for new messages
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                // Check the currently assigned partitions
                Set<TopicPartition> assignedPartitions = consumer.assignment();
                if (!assignedPartitions.isEmpty()) {
                    System.out.println("Currently assigned partitions: " + assignedPartitions);
                } else {
                    System.out.println("No partitions currently assigned.");
                }
                
                // Process the records
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumer [%s]: Sync - Consumed message with key=%s, value=%s, partition=%d, offset=%d%n",
                            consumerName, record.key(), record.value(), record.partition(), record.offset());
                }

                // Commit offsets synchronously
                consumer.commitSync();
                System.out.printf("Consumer [%s]: Sync - Offsets committed.%n", consumerName);
            }
        } catch (Exception e) {
            System.err.printf("Consumer [%s]: Sync - Error during consuming: %s%n", consumerName, e.getMessage());
        }
    }

    /**
     * Consumes messages asynchronously with manual commit.
     */
    private static void consumeAsynchronously(Consumer<String, String> consumer, String consumerName) {
        System.out.printf("Consumer [%s]: Consuming messages asynchronously...%n", consumerName);
        try {
            while (true) {
                // Poll for new messages
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                // Check the currently assigned partitions
                Set<TopicPartition> assignedPartitions = consumer.assignment();
                if (!assignedPartitions.isEmpty()) {
                    System.out.println("Currently assigned partitions: " + assignedPartitions);
                } else {
                    System.out.println("No partitions currently assigned.");
                }
                
                // Process the records
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumer [%s]: Async - Consumed message with key=%s, value=%s, partition=%d, offset=%d%n",
                            consumerName, record.key(), record.value(), record.partition(), record.offset());
                }

                // Commit offsets asynchronously
                consumer.commitAsync((offsets, exception) -> {
                    if (exception == null) {
                        System.out.printf("Consumer [%s]: Async - Offsets committed successfully.%n", consumerName);
                    } else {
                        System.err.printf("Consumer [%s]: Async - Error committing offsets: %s%n", consumerName, exception.getMessage());
                    }
                });
            }
        } catch (Exception e) {
            System.err.printf("Consumer [%s]: Async - Error during consuming: %s%n", consumerName, e.getMessage());
        } finally {
            // Ensure final commit if required
            try {
                consumer.commitSync();
                System.out.printf("Consumer [%s]: Async - Final offsets committed.%n", consumerName);
            } catch (Exception e) {
                System.err.printf("Consumer [%s]: Async - Error during final offset commit: %s%n", consumerName, e.getMessage());
            }
        }
    }
}