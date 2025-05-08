package com.example;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.connection.ClusterConnectionMode;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MongoDBConfig {
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "password";
    private static final String DATABASE = "flinkdb";
    private static final String HOST = "mongodb"; // "mongodb";
    private static final int PORT = 27017; // 27017;

    public static MongoClient createMongoClient() {
        // Use connection string authentication for simplicity and reliability
        String connectionString = String.format(
            "mongodb://%s:%s@%s:%d/%s?authSource=admin", 
            USERNAME, PASSWORD, HOST, PORT, DATABASE
        );
        
        log.info("Connecting to MongoDB with connection string template:{}", connectionString);
        
        // Create client with connection string
        return MongoClients.create(connectionString);
    }
} 