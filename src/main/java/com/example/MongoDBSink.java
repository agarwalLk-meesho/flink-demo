package com.example;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBSink extends RichSinkFunction<TransactionAggregate> {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSink.class);
    
    private transient MongoClient mongoClient;
    private transient MongoCollection<Document> collection;
    private final String database;
    private final String collectionName;

    public MongoDBSink(String database, String collectionName) {
        this.database = database;
        this.collectionName = collectionName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        mongoClient = MongoDBConfig.createMongoClient();
        MongoDatabase db = mongoClient.getDatabase(database);
        collection = db.getCollection(collectionName);
        LOG.info("Connected to MongoDB database: {}, collection: {}", database, collectionName);
    }

    @Override
    public void invoke(TransactionAggregate value, Context context) throws Exception {
        LOG.info("Invoking MongoDBSink with value: {}", value);
        Document doc = new Document()
                .append("category", value.getCategory())
                .append("totalAmount", value.getTotalAmount())
                .append("transactionCount", value.getTransactionCount())
                .append("windowEnd", value.getWindowEnd());
        collection.insertOne(doc);
        LOG.info("Inserted document: {}", doc);
    }

    @Override
    public void close() throws Exception {
        if (mongoClient != null) {
            mongoClient.close();
            LOG.info("Closed MongoDB connection");
        }
    }
} 