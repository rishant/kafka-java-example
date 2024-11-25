package com.example.native_kafka_poc.spark_stream;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class KafkaSparkSqlStreamingExample {
	
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {

        // Create SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("KafkaSparkStreamingExample")
                .master("local[*]")  // Use local[*] for local execution
                .getOrCreate();

        // Set log level to WARN
        spark.sparkContext().setLogLevel("WARN");

        // Read data from Kafka
        Dataset<Row> kafkaDF = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092") // Kafka broker address
                .option("subscribe", "test-topic") // Kafka topic to subscribe to
                .option("startingOffsets", "latest") // Start reading from latest offset
                .load();

        // Transform Kafka messages (key and value are binary by default)
        Dataset<Row> transformedDF = kafkaDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        // Write to console
        StreamingQuery query = transformedDF.writeStream()
                .outputMode("append") // Options: append, update, complete
                .format("console")
                .start();

        // Wait for the query to terminate
        query.awaitTermination();
    }
}