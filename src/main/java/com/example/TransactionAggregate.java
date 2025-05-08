package com.example;

import java.io.Serializable;

public class TransactionAggregate implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String category;
    private double totalAmount;
    private int transactionCount;
    private long windowEnd;

    public TransactionAggregate() {
        this.totalAmount = 0.0;
        this.transactionCount = 0;
    }

    // Getters and Setters
    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }
    public double getTotalAmount() { return totalAmount; }
    public void setTotalAmount(double totalAmount) { this.totalAmount = totalAmount; }
    public int getTransactionCount() { return transactionCount; }
    public void setTransactionCount(int transactionCount) { this.transactionCount = transactionCount; }
    public long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }

    @Override
    public String toString() {
        return "TransactionAggregate{" +
                "category='" + category + '\'' +
                ", totalAmount=" + totalAmount +
                ", transactionCount=" + transactionCount +
                ", windowEnd=" + windowEnd +
                '}';
    }
} 