package com.pipeline.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class SparkStreamingProcessor implements CommandLineRunner {

    @Override
    public void run(String... args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("USGSWaterDataProcessor")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .config("spark.ui.liveUpdate", "false")
                .getOrCreate();

        // 1. Read Stream from Kafka
        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "usgs-water-metrics")
                .option("startingOffsets", "earliest")
                .load();

        // 2. Define schema for standard top-level JSON fields
        StructType usgsSchema = new StructType()
                .add("siteCode", DataTypes.StringType)
                .add("site_code", DataTypes.StringType)
                .add("timestamp", DataTypes.StringType)
                .add("dateTime", DataTypes.StringType)
                .add("value", DataTypes.StringType);

        // 3. Extract columns from Struct OR fallback to JSON path evaluation on the raw
        // JSON string
        Dataset<Row> parsedMetrics = kafkaStream
                .selectExpr("CAST(value AS STRING) as json_string")
                .select(
                        functions.from_json(functions.col("json_string"), usgsSchema).as("data"),
                        functions.col("json_string"))
                .select(
                        functions.coalesce(
                                functions.col("data.siteCode"),
                                functions.col("data.site_code"),
                                functions.get_json_object(functions.col("json_string"), "$.siteCode"),
                                functions.get_json_object(functions.col("json_string"), "$.site_code")).as("site_code"),

                        functions.to_timestamp(
                                functions.coalesce(
                                        functions.col("data.timestamp"),
                                        functions.col("data.dateTime"),
                                        functions.get_json_object(functions.col("json_string"), "$.timestamp"),
                                        functions.get_json_object(functions.col("json_string"), "$.dateTime")))
                                .as("timestamp"),

                        functions.coalesce(
                                functions.col("data.value"),
                                functions.get_json_object(functions.col("json_string"), "$.value"))
                                .cast(DataTypes.DoubleType).as("value"));

        // 4. JDBC Write properties
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "pipeline_user");
        connectionProperties.put("password", "pipeline_password");
        connectionProperties.put("driver", "org.postgresql.Driver");

        StreamingQuery query = parsedMetrics.writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    if (!batchDF.isEmpty()) {
                        System.out.println("=== PROCESSING BATCH " + batchId + " ===");
                        batchDF.show(5, false);

                        batchDF.write()
                                .mode("append")
                                .jdbc("jdbc:postgresql://localhost:5432/reporting_db", "water_metrics",
                                        connectionProperties);
                    }
                })
                .option("checkpointLocation", "/tmp/spark-kafka-checkpoint-" + System.currentTimeMillis())
                .start();
    }
}