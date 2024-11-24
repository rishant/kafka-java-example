package com.example.native_kafka_poc.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class LongRunningTaskConsumerWithRebalanceExample2 {
    private static final String TOPIC = "my-long-task-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "optimized-long-running-task-group";

    private static final int MAX_CONCURRENT_TASKS = 5; // Limit concurrent tasks
    private static final ExecutorService executor = Executors.newFixedThreadPool(MAX_CONCURRENT_TASKS);
    private static final Map<TopicPartition, OffsetAndMetadata> checkpointOffsets = new ConcurrentHashMap<>();
    private static final Semaphore semaphore = new Semaphore(MAX_CONCURRENT_TASKS); // Backpressure control

    public static void main(String[] args) {
        // Step 1: Configure Kafka Consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Disable auto-commit
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10"); // Fetch multiple records

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC), new RebalanceListener(consumer));

            while (true) {
                // Step 2: Poll for records
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                if (records.isEmpty()) {
                    System.out.println("No records found. Waiting...");
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    // Step 3: Backpressure control
                    try {
                        semaphore.acquire(); // Block if too many tasks are running
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Semaphore interrupted", e);
                    }

                    // Step 4: Submit task to executor
                    executor.submit(() -> {
                        try {
                            processRecord(record);

                            // Step 5: Track offsets for manual commit
                            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                            checkpointOffsets.put(partition, new OffsetAndMetadata(record.offset() + 1));
                        } catch (Exception e) {
                            System.err.printf("Error processing record: Key=%s, Value=%s, Error=%s%n",
                                    record.key(), record.value(), e.getMessage());
                        } finally {
                            semaphore.release(); // Release semaphore after task completion
                        }
                    });
                }

                // Step 6: Commit offsets for completed tasks
                commitOffsets(consumer);
            }
        } finally {
            executor.shutdown();
        }
    }

    // Simulate a long-running task
    private static void processRecord(ConsumerRecord<String, String> record) throws InterruptedException {
        System.out.printf("Processing record: Key=%s, Value=%s%n", record.key(), record.value());
        Thread.sleep(5000); // Simulating 5 seconds of processing time
        System.out.println("Record processing completed.");
    }

    // Commit offsets for completed tasks
    private static void commitOffsets(KafkaConsumer<String, String> consumer) {
        try {
            if (!checkpointOffsets.isEmpty()) {
                consumer.commitSync(checkpointOffsets);
                System.out.println("Offsets committed: " + checkpointOffsets);
                checkpointOffsets.clear(); // Clear checkpointed offsets after commit
            }
        } catch (Exception e) {
            System.err.println("Error committing offsets: " + e.getMessage());
        }
    }

    // Custom Rebalance Listener
    private static class RebalanceListener implements ConsumerRebalanceListener {
        private final KafkaConsumer<String, String> consumer;

        public RebalanceListener(KafkaConsumer<String, String> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            System.out.println("Partitions revoked. Committing offsets before rebalance...");
            commitOffsets(consumer); // Commit offsets before rebalance
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.out.printf("Partitions assigned: %s%n", partitions);
        }
    }
}
