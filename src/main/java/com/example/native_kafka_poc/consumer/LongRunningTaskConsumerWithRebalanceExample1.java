package com.example.native_kafka_poc.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LongRunningTaskConsumerWithRebalanceExample1 {
    private static final String TOPIC = "my-long-task-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "long-running-task-group";

    // Track uncommitted offsets for each partition
    private static final Map<TopicPartition, OffsetAndMetadata> uncommittedOffsets = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        // Step 1: Configure Kafka Consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Disable auto-commit
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"); // Process one record at a time

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // Step 2: Subscribe with a Rebalance Listener
            consumer.subscribe(Collections.singletonList(TOPIC), new RebalanceListener(consumer));

            while (true) {
                // Step 3: Poll for records
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                if (records.isEmpty()) {
                    System.out.println("No records found. Waiting...");
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Received record: Key=%s, Value=%s, Partition=%d, Offset=%d%n",
                            record.key(), record.value(), record.partition(), record.offset());

                    // Track the current offset before processing
                    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                    uncommittedOffsets.put(partition, new OffsetAndMetadata(record.offset() + 1));

                    // Step 4: Pause partitions while processing
                    Set<TopicPartition> assignedPartitions = consumer.assignment();
                    consumer.pause(assignedPartitions);
                    System.out.println("Polling paused for assigned partitions.");

                    try {
                        // Simulate a long-running task
                        processRecord(record);
                    } catch (Exception e) {
                        System.err.printf("Error processing record: Key=%s, Value=%s, Error=%s%n",
                                record.key(), record.value(), e.getMessage());
                    } finally {
                        // Step 5: Resume polling after processing
                        consumer.resume(assignedPartitions);
                        System.out.println("Polling resumed for assigned partitions.");
                    }

                    // Step 6: Commit the offset manually
                    consumer.commitSync(uncommittedOffsets);
                    System.out.printf("Offset committed for record: Partition=%d, Offset=%d%n",
                            record.partition(), record.offset());
                }
            }
        }
    }

    // Simulate a long-running task
    private static void processRecord(ConsumerRecord<String, String> record) throws InterruptedException {
        System.out.printf("Processing record: Key=%s, Value=%s%n", record.key(), record.value());
        Thread.sleep(5000); // Simulating 5 seconds of processing time
        System.out.println("Record processing completed.");
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
            consumer.commitSync(uncommittedOffsets); // Commit tracked offsets
            uncommittedOffsets.clear(); // Clear the uncommitted offsets map
            System.out.println("Offsets committed successfully before rebalance.");
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.out.printf("Partitions assigned: %s%n", partitions);
        }
    }
}