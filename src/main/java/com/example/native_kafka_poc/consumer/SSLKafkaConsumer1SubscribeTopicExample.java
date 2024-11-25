package com.example.native_kafka_poc.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SSLKafkaConsumer1SubscribeTopicExample {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CONSUMER_GROUP_NAME = "consumer_group_execution_data_1";
    private static final String ENABLE_AUTO_COMMIT = "false";
    private static final String MAX_POLL_RECORDS = "1";
    private static final String TOPIC_NAME = "execution-data";

    public static void main(String[] args) {
        // Configure Kafka consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Developer testing

        /* Kafka SSL Configuration */
		props.put("security.protocol","SSL");
		props.put("ssl.truststore.location","<path>/server.truststore.jks");
		props.put("ssl.truststore.password","kafka123");
		props.put("ssl.keystore.location","<path>/server.keystore.jks");
		props.put("ssl.keystore.password","kafka123");
		props.put("ssl.key.password","kafka123");
		
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
        	consumer.subscribe(Collections.singletonList(TOPIC_NAME));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

                records.forEach(record -> {
                    // Print consumed record details
                    System.out.printf("Consumed record with key %s and value %s from partition %d, offset %d%n",
                            record.key(), record.value(), record.partition(), record.offset());

                    // Prepare the offsets for manual commit
                    offsetsToCommit.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, null)
                    );
                });

                // Commit offsets if there are any to commit
                if (!offsetsToCommit.isEmpty()) {
                    consumer.commitSync(offsetsToCommit);
                    System.out.println("Offsets committed successfully.");
                }
            }
        } catch (Exception e) {
            System.err.println("Error while consuming records: " + e.getMessage());
        }
    }
}
