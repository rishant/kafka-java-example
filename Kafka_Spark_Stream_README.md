
# Kafka-Spark Streaming Integration Example (Java)

This example demonstrates how to integrate Apache Spark with Apache Kafka to process real-time data streams using Spark Structured Streaming.

---

## Prerequisites
1. **Apache Kafka**: A running Kafka broker and topic (e.g., `test-topic`).
2. **Apache Spark**: Installed Spark environment.
3. **Java**: Installed Java environment and Maven for dependency management.

---

## Maven Dependencies

Add the following dependencies to your `pom.xml`:

```xml
<dependencies>
    <!-- Spark Core and SQL -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.12</artifactId>
        <version>3.5.0</version>
    </dependency>

    <!-- Spark Kafka Integration -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql-kafka-0-10_2.12</artifactId>
        <version>3.5.0</version>
    </dependency>

    <!-- Kafka Clients -->
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
        <version>3.5.1</version>
    </dependency>
</dependencies>
```

---

## Java Code Example

Here is the Java implementation:

```java
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class KafkaSparkStreaming {
    public static void main(String[] args) throws StreamingQueryException {

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
```

---

## Steps to Run

1. **Start Kafka Broker**:
   ```bash
   bin/zookeeper-server-start.sh config/zookeeper.properties
   bin/kafka-server-start.sh config/server.properties
   ```

2. **Create Kafka Topic**:
   ```bash
   bin/kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

3. **Start Kafka Producer**:
   ```bash
   bin/kafka-console-producer.sh --topic test-topic --bootstrap-server localhost:9092
   ```
   Produce some messages like:
   ```
   hello spark
   real-time streaming with kafka
   ```

4. **Build and Run Spark Application**:
   ```bash
   mvn clean package
   spark-submit --class KafkaSparkStreaming --master local[*] target/your-jar-file-name.jar
   ```

---

## Explanation

1. **Kafka DataFrame**: Data is read using the `spark.readStream().format("kafka")` API.
2. **Transformations**: Extract `key` and `value` fields from Kafka messages and process them.
3. **Output Mode**: `append` mode writes only new rows.
4. **Streaming Sink**: Data is written to the console using `.format("console")`.

---

## Enhancements
- Save data to HDFS or S3 using `.format("parquet").option("path", "hdfs://path/to/output").start()`.
- Implement time-window aggregations:
  ```java
  Dataset<Row> windowed = transformedDF.groupBy(
      functions.window(col("timestamp"), "10 minutes"), col("key")
  ).count();
  ```
---

Feel free to modify this example to suit your specific use case!
