package com.example.native_kafka_poc.spark_stream;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Transaction {
    @JsonProperty("transactionId")
    private String transactionId;

    @JsonProperty("amount")
    private double amount;

    @JsonProperty("type")
    private String type;

    @JsonProperty("timestamp")
    private long timestamp;

    // Getters and Setters
    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
