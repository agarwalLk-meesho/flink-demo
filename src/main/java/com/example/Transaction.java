package com.example;

import java.io.Serializable;

public class Transaction implements Serializable {
    private String transactionId;
    private String userId;
    private double amount;
    private String category;
    private long timestamp;

    public Transaction() {}

    public Transaction(String transactionId, String userId, double amount, String category, long timestamp) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.amount = amount;
        this.category = category;
        this.timestamp = timestamp;
    }

    // Getters and Setters
    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }
    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "Transaction{" +
                "transactionId='" + transactionId + '\'' +
                ", userId='" + userId + '\'' +
                ", amount=" + amount +
                ", category='" + category + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
} 