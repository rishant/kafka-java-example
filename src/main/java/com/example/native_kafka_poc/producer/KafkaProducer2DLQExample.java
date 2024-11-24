package com.example.native_kafka_poc.producer;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducer2DLQExample {

	public static void main(String[] args) {
        // Kafka configuration properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Replace with your Kafka broker address
        
        String producerName = "KafkaProducerExternalRetryExample-" + UUID.randomUUID(); // Producer unique identifier
        props.put("client.id", producerName);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("acks", "all"); // Wait for all replicas to acknowledge
                
		Producer<String, String> producer = new KafkaProducer<>(props);

		ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "key", "value");
		
		producer.send(record, (metadata, exception) -> {
		    if (exception != null) {
		        // Send to DLQ
		        ProducerRecord<String, String> dlqRecord = new ProducerRecord<>("dlq-topic", record.key(), record.value());
		        producer.send(dlqRecord);
		        System.err.println("Failed to send message. Sent to DLQ.");
		    } else {
		        System.out.printf("Message sent successfully to partition %d with offset %d%n",
		                          metadata.partition(), metadata.offset());
		    }
		});
		
		producer.close();
	}
}
