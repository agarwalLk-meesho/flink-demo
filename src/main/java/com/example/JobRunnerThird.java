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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

public class JobRunnerThird {
    private static final Logger LOG = LoggerFactory.getLogger(JobRunnerThird.class);
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
        // Enable checkpointing for state persistence
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///opt/flink/checkpoints");        
        // Kafka source configuration
        KafkaSource<String> source = KafkaSource.<String>builder()
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
        // Create transaction aggregates using event time windows and reduce
        DataStream<TransactionAggregate> aggregates = transactions
                .map(tx -> {
                    // Convert each Transaction to a TransactionAggregate
                    TransactionAggregate agg = new TransactionAggregate();
                    agg.setCategory(tx.getCategory());
                    agg.setTotalAmount(tx.getAmount());
                    agg.setTransactionCount(1);
                    agg.setWindowEnd(tx.getTimestamp()); // Use transaction timestamp
                    LOG.info("Mapped transaction to aggregate: {}", agg);
                    return agg;
                })
                .keyBy(TransactionAggregate::getCategory)  // Group by category
                .window(TumblingEventTimeWindows.of(Time.seconds(5))) // 5-second event time windows
                .reduce(new ReduceFunction<TransactionAggregate>() {
                    @Override
                    public TransactionAggregate reduce(TransactionAggregate agg1, TransactionAggregate agg2) {
                        // Combine two aggregates
                        TransactionAggregate result = new TransactionAggregate();
                        result.setCategory(agg1.getCategory());
                        result.setTotalAmount(agg1.getTotalAmount() + agg2.getTotalAmount());
                        result.setTransactionCount(agg1.getTransactionCount() + agg2.getTransactionCount());
                        result.setWindowEnd(Math.max(agg1.getWindowEnd(), agg2.getWindowEnd())); // Use latest timestamp
                        
                        LOG.info("Reduced aggregates: {} + {} = {}", agg1, agg2, result);
                        return result;
                    }
                });
        // Send to MongoDB
        aggregates.addSink(new MongoDBSink("flinkdb", "transaction_aggregates"));
        LOG.info("Added MongoDB sink for 5-second event time windowed aggregates");
 
        // Execute the job
        env.execute("Transaction Aggregation with 5-Second Event Time Windows");
    }
}