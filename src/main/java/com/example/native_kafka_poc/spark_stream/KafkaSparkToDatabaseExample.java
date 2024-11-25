package com.example.native_kafka_poc.spark_stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeoutException;

public class KafkaSparkToDatabaseExample {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {

        // Create SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("KafkaToDatabase")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Kafka source
        Dataset<Row> kafkaStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "transaction-topic")
                .option("startingOffsets", "latest")
                .load();

        // Deserialize the Kafka value (assumed to be JSON) to Transaction object
        ObjectMapper objectMapper = new ObjectMapper();

        kafkaStream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").foreach(row -> {
            String key = row.getString(0);
            String value = row.getString(1);

            Transaction transaction = objectMapper.readValue(value, Transaction.class);

            // Process transaction with optimistic or pessimistic locking
            processTransactionWithLocks(transaction);
        });

        // Start the stream
        StreamingQuery query = kafkaStream.writeStream()
                .outputMode("append")
                .format("console")
                .start();

        query.awaitTermination();
    }

    private static void processTransactionWithLocks(Transaction transaction) {
        // Database connection details
        String jdbcUrl = "jdbc:mysql://localhost:3306/your_database";
        String jdbcUser = "root";
        String jdbcPassword = "password";

        try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)) {
            connection.setAutoCommit(false);

            // Example of optimistic locking
            String selectQuery = "SELECT balance FROM accounts WHERE account_id = ?";
            try (PreparedStatement selectStmt = connection.prepareStatement(selectQuery)) {
                selectStmt.setString(1, transaction.getTransactionId());
                ResultSet resultSet = selectStmt.executeQuery();

                if (resultSet.next()) {
                    double currentBalance = resultSet.getDouble("balance");

                    if (transaction.getAmount() > currentBalance) {
                        throw new Exception("Insufficient balance for transaction.");
                    }

                    // Update balance (pessimistic lock via transaction)
                    String updateQuery = "UPDATE accounts SET balance = balance - ? WHERE account_id = ?";
                    try (PreparedStatement updateStmt = connection.prepareStatement(updateQuery)) {
                        updateStmt.setDouble(1, transaction.getAmount());
                        updateStmt.setString(2, transaction.getTransactionId());
                        updateStmt.executeUpdate();
                    }
                }

                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}