package com.pipeline.service;

import com.pipeline.config.KafkaConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

@Component
public class SparkStreamingProcessor implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamingProcessor.class);

    private SparkSession spark;
    private StreamingQuery query;

    @Override
    public void run(String... args) {
        // Run in background so Spring Boot startup is never blocked or crashed if Kafka/Postgres is initializing
        CompletableFuture.runAsync(this::startStreamingPipeline);
    }

    public synchronized void startStreamingPipeline() {
        try {
            log.info("Initializing Spark Streaming Pipeline...");

            spark = SparkSession.builder()
                    .appName("USGSWaterDataProcessor")
                    .master("local[*]")
                    .config("spark.ui.enabled", "false")
                    .config("spark.ui.liveUpdate", "false")
                    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
                    .getOrCreate();

            // Set Hadoop client config for local execution
            spark.sparkContext().hadoopConfiguration().set("dfs.client.use.datanode.hostname", "true");

            // 1. Read Stream from Kafka
            Dataset<Row> kafkaStream = spark.readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", KafkaConfig.USGS_TOPIC)
                    .option("startingOffsets", "earliest")
                    .option("failOnDataLoss", "false")
                    .load();

            // 2. Define schema matching the discrete event published by UsgsDataIngestionService
            StructType usgsSchema = new StructType()
                    .add("siteCode", DataTypes.StringType)
                    .add("site_code", DataTypes.StringType)
                    .add("siteName", DataTypes.StringType)
                    .add("timestamp", DataTypes.StringType)
                    .add("dateTime", DataTypes.StringType)
                    .add("value", DataTypes.DoubleType);

            // 3. Parse JSON and project target fields
            Dataset<Row> parsedMetrics = kafkaStream
                    .selectExpr("CAST(value AS STRING) as json_string")
                    .select(functions.from_json(functions.col("json_string"), usgsSchema).as("data"))
                    .select(
                            functions.coalesce(
                                    functions.col("data.siteCode"),
                                    functions.col("data.site_code")
                            ).as("site_code"),

                            functions.to_timestamp(
                                    functions.coalesce(
                                            functions.col("data.timestamp"),
                                            functions.col("data.dateTime")
                                    )
                            ).as("timestamp"),

                            functions.col("data.value").as("value")
                    )
                    .filter("site_code IS NOT NULL AND timestamp IS NOT NULL AND value IS NOT NULL");

            // 4. JDBC Sink Configuration
            Properties connectionProperties = new Properties();
            connectionProperties.put("user", "pipeline_user");
            connectionProperties.put("password", "pipeline_password");
            connectionProperties.put("driver", "org.postgresql.Driver");

            query = parsedMetrics.writeStream()
                    .foreachBatch((batchDF, batchId) -> {
                        if (!batchDF.isEmpty()) {
                            long count = batchDF.count();
                            log.info("=== PROCESSING SPARK STREAM BATCH {} ({} records) ===", batchId, count);
                            batchDF.show(5, false);

                            batchDF.write()
                                    .mode("append")
                                    .jdbc("jdbc:postgresql://localhost:5432/reporting_db", "water_metrics", connectionProperties);

                            log.info("=== BATCH {} WRITTEN TO POSTGRESQL ===", batchId);
                        }
                    })
                    .option("checkpointLocation", "/tmp/spark-kafka-checkpoint")
                    .start();

            log.info("Spark Streaming query started successfully: {}", query.id());
        } catch (Exception e) {
            log.error("Spark Streaming initialization failed (Kafka/Postgres may not be running yet): {}", e.getMessage(), e);
        }
    }

    public boolean isQueryActive() {
        return query != null && query.isActive();
    }

    @PreDestroy
    public void shutdown() {
        if (query != null && query.isActive()) {
            try {
                log.info("Stopping Spark Streaming Query...");
                query.stop();
            } catch (Exception e) {
                log.warn("Error stopping streaming query: {}", e.getMessage());
            }
        }
    }
}
