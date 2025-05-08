package com.example;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
// import org.apache.flink.connector.kafka.source.OffsetsInitializer.OffsetResetStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import org.apache.commons.lang3.StringUtils;

import com.esotericsoftware.minlog.Log;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

public class JobRunnerSecond {
    private static final Logger LOG = LoggerFactory.getLogger(JobRunnerSecond.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public void run() throws Exception {
        // Configure logging
        // ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) 
        // LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        // root.setLevel(ch.qos.logback.classic.Level.INFO);
        
        // Flink setup
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081");
        // final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Enable checkpointing for state persistence - use a shorter interval for more frequent checkpoints
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///opt/flink/checkpoints");  

        // Kafka source configuration
        KafkaSource<String> source = KafkaSource.<String>builder()
                // .setBootstrapServers("localhost:9092")
                .setBootstrapServers("kafka:29092")
                .setTopics("transactions")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create the stream from Kafka
        DataStream<String> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        LOG.info("Reading from Kafka stream: {}", kafkaStream);

        // Parse JSON to Transaction objects with event time
        DataStream<Transaction> transactions = kafkaStream
                .map(json -> {
                    LOG.info("Received from Kafka: {}", json);
                    try {
                        return objectMapper.readValue(json, Transaction.class);
                    } catch (Exception e) {
                        LOG.error("Failed to parse JSON: {}", json, e);
                        return null;
                    }
                })
                .filter(t -> t != null)
                // Assign timestamps and generate watermarks from the transaction's timestamp field
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                        .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((SerializableTimestampAssigner<Transaction>) (tx, recordTimestamp) -> tx.getTimestamp())
                );

        LOG.info("Assigned timestamps and watermarks to transactions");

        // Create aggregates using 5-second windows and a global window function for reliable checkpointing
        // windowAll processes ALL events in a window, regardless of key, in a single function call
        DataStream<TransactionAggregate> aggregates = transactions
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            .process(new AllTransactionsWindowAggregator());

        // Send to MongoDB
        aggregates.addSink(new MongoDBSink("flinkdb", "transaction_aggregates"));
        LOG.info("Added MongoDB sink for 5-second event time windowed aggregates with windowAll");
 
        // Execute the job
        env.execute("Transaction Aggregation with 5-Second Global Windows and Reliable Checkpointing");
    }
    
    /**
     * Process ALL window function that handles all transactions in a window across ALL categories.
     * This approach provides more reliable checkpointing of window state by processing all events together.
     * 
     * The key difference from a regular ProcessWindowFunction:
     * - windowAll() puts ALL events into the same window, regardless of key
     * - We manually group events by category inside the process method
     * - This ensures all window state is checkpointed together
     */
    public static class AllTransactionsWindowAggregator 
            extends ProcessAllWindowFunction<Transaction, TransactionAggregate, TimeWindow> {
        
        @Override
        public void process(
                Context context,
                Iterable<Transaction> transactions, 
                Collector<TransactionAggregate> out) throws Exception {
            
            LOG.info("Processing ALL transactions for window: {} to {}", 
                    context.window().getStart(), context.window().getEnd());
            
            // Group transactions by category
            Map<String, List<Transaction>> transactionsByCategory = new HashMap<>();
            
            // First pass - group all transactions by category
            for (Transaction tx : transactions) {
                String category = tx.getCategory();
                if (!transactionsByCategory.containsKey(category)) {
                    transactionsByCategory.put(category, new ArrayList<>());
                }
                transactionsByCategory.get(category).add(tx);
                LOG.info("Grouped transaction: {} under category: {}", tx, category);
            }
            
            // Second pass - create aggregates for each category
            for (Map.Entry<String, List<Transaction>> entry : transactionsByCategory.entrySet()) {
                String category = entry.getKey();
                List<Transaction> categoryTransactions = entry.getValue();
                
                TransactionAggregate result = new TransactionAggregate();
                result.setCategory(category);
                result.setTotalAmount(0.0);
                result.setTransactionCount(0);
                result.setWindowEnd(context.window().getEnd());
                
                // Aggregate all transactions for this category
                for (Transaction tx : categoryTransactions) {
                    result.setTotalAmount(result.getTotalAmount() + tx.getAmount());
                    result.setTransactionCount(result.getTransactionCount() + 1);
                }
                
                LOG.info("Emitting aggregate for category '{}': {}", category, result);
                out.collect(result);
            }
        }
    }
}