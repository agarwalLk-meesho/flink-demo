package com.example;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import org.apache.commons.lang3.StringUtils;

public class JobRunnerFirst {  
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(JobRunnerFirst.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();


    public void run() throws Exception {
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) 
        LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        root.setLevel(ch.qos.logback.classic.Level.INFO);
        // Create configuration for web UI
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081");

        // Create local environment with web UI
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(1);
        
        // Set watermark interval to a smaller value (200ms) for faster watermark propagation
        env.getConfig().setAutoWatermarkInterval(200);
        
        LOG.info("Starting Flink application with event time processing");

        // Create Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("transactions")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create data stream from Kafka
        DataStream<String> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // Parse JSON to Transaction objects with better error handling
        DataStream<Transaction> transactions = kafkaStream
                .map(json -> {
                    // Skip empty messages without logging errors
                    if (json == null || json.trim().isEmpty()) {
                        LOG.debug("Skipping empty message");
                        return null;
                    }
                    
                    try {
                        Transaction t = objectMapper.readValue(json, Transaction.class);
                        LOG.info("Parsed transaction: {} with timestamp {}", t, t.getTimestamp());
                        
                        // Validate timestamp (Unix epoch in milliseconds should be > 0)
                        if (t.getTimestamp() <= 0) {
                            LOG.warn("Transaction has invalid timestamp ({}), setting current time", t.getTimestamp());
                            t.setTimestamp(System.currentTimeMillis());
                        }
                        
                        return t;
                    } catch (Exception e) {
                        LOG.error("Failed to parse JSON: {} - Error: {}", json, e.getMessage());
                        return null;
                    }
                })
                .filter(transaction -> transaction != null);

        // Define watermark strategy using the transaction timestamp
        // Improved watermark strategy with periodic watermark generation
        WatermarkStrategy<Transaction> watermarkStrategy = WatermarkStrategy
                .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((transaction, timestamp) -> transaction.getTimestamp())
                // Add idle timeout to handle cases where no events arrive for a while
                .withIdleness(Duration.ofSeconds(10));

        // Process transactions and create aggregates with event time
        DataStream<TransactionAggregate> aggregates = transactions
                .assignTimestampsAndWatermarks(watermarkStrategy)  // Use transaction timestamp
                .keyBy(Transaction::getCategory)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                // Allow late events to be processed even after window closes
                .allowedLateness(Time.seconds(5))
                .aggregate(new TransactionAggregator());

        LOG.info("Creating event-time windowed aggregations with 30-second windows");

        // Write to MongoDB using custom sink
        aggregates.addSink(new MongoDBSink(
                "flinkdb",
                "transaction_aggregates"
        ));

        // Execute the job
        LOG.info("Starting Flink job");
        env.execute("Transaction Processing Job");
    }

    private static class TransactionAggregator implements AggregateFunction<Transaction, TransactionAggregate, TransactionAggregate> {
        private static final long serialVersionUID = 1L;
        
        @Override
        public TransactionAggregate createAccumulator() {
            LOG.debug("Creating new transaction accumulator");
            return new TransactionAggregate();
        }

        @Override
        public TransactionAggregate add(Transaction transaction, TransactionAggregate accumulator) {
            LOG.info("Adding transaction {} (amount={}, timestamp={}) to window for category '{}'", 
                     transaction.getTransactionId(), 
                     transaction.getAmount(),
                     transaction.getTimestamp(),
                     transaction.getCategory());
                     
            accumulator.setCategory(transaction.getCategory());
            accumulator.setTotalAmount(accumulator.getTotalAmount() + transaction.getAmount());
            accumulator.setTransactionCount(accumulator.getTransactionCount() + 1);
            
            // Use max timestamp for the window to ensure latest event time
            accumulator.setWindowEnd(Math.max(accumulator.getWindowEnd(), transaction.getTimestamp()));
            
            LOG.debug("Updated accumulator: {}", accumulator);
            return accumulator;
        }

        @Override
        public TransactionAggregate getResult(TransactionAggregate accumulator) {
            LOG.info("Window complete - Category: {}, Total: {}, Count: {}, WindowEnd: {}", 
                     accumulator.getCategory(), 
                     accumulator.getTotalAmount(), 
                     accumulator.getTransactionCount(),
                     accumulator.getWindowEnd());
            return accumulator;
        }

        @Override
        public TransactionAggregate merge(TransactionAggregate a, TransactionAggregate b) {
            LOG.debug("Merging two accumulators");
            a.setTotalAmount(a.getTotalAmount() + b.getTotalAmount());
            a.setTransactionCount(a.getTransactionCount() + b.getTransactionCount());
            // Use the latest timestamp when merging
            a.setWindowEnd(Math.max(a.getWindowEnd(), b.getWindowEnd()));
            return a;
        }
    }

}
