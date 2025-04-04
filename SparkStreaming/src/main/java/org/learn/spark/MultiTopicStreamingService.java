package org.learn.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class MultiTopicStreamingService {

    private final SparkSession spark;
    private final String bootstrapServers;
    private StreamingQuery objectCassandraQuery;
    private StreamingQuery stringCassandraQuery;

    public MultiTopicStreamingService(SparkSession spark, String bootstrapServers) {
        this.spark = spark;
        this.bootstrapServers = bootstrapServers;
    }

    private static final StructType PRODUCT_SCHEMA = new StructType()
            .add("id", DataTypes.StringType)      // ID string format mein
            .add("name", DataTypes.StringType)    // Product name string
            .add("category", DataTypes.StringType) // Category string (e.g., "Electronics")
            .add("description", DataTypes.StringType, true)  // Last arg `true` = nullable // Description text
            .add("quantity", DataTypes.IntegerType) // Stock (integer)
            .add("price", DataTypes.DoubleType)    // Price (decimal)
            .add("created_at", DataTypes.StringType); // Date (pehle string, baad mein convert)

    public void startObjectTopicStream() throws Exception {
        // 1. Kafka se data stream karo
        Dataset<Row> kafkaData = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", "object-topic")
                .option("startingOffsets", "latest")  // Sirf naye messages lo
//                .option("startingOffsets", "earliest")  // ✅ Purana data bhi process hoga
                .option("failOnDataLoss", "false")      // ✅ Chota sa data loss tolerate karo
                .load();

        // 3. JSON ko parse karo (PRODUCT_SCHEMA use karke)
        Dataset<Row> parsedData = kafkaData
                .selectExpr("CAST(value AS STRING) as json")
                .select(from_json(col("json"), PRODUCT_SCHEMA).as("data"))
                .select("data.*");

        // 4. FILTER LAGAO (Electronics, price > 30k, stock >= 10)
        Dataset<Row> filteredData = parsedData.filter(
                //    col("category").equalTo("Electronics")  // Single Category Electronics
                col("category").isin("Electronics", "Computers", "Mobiles") // Multiple Category filter
                        .and(col("price").gt(300))           // Price > 30k
                        .and(col("quantity").geq(10))          // Stock >= 10
        );

        // 5. Console pe print karo (DEBUG ke liye)
        StreamingQuery consoleQuery = filteredData.writeStream()
                .outputMode("append")
                .format("console")
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start();

        // 6. Cassandra mein save karo
        this.objectCassandraQuery = filteredData.writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    batchDF.write()
                            .format("org.apache.spark.sql.cassandra")
                            .option("keyspace", "product_ks")
                            .option("table", "products")
                            .mode("append") // Overwrite/Append/Ignore
                            .save();
                })
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

    }

    public void startStringTopicStream() throws Exception {
        // 1. Read from Kafka
        Dataset<Row> kafkaStreamDF = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", "string-topic")
                .option("startingOffsets", "latest")
                .load();

        // 2. Process the string data
        Dataset<Row> processedDF = kafkaStreamDF
                .selectExpr("CAST(value AS STRING) as raw_value")
                .withColumn("processed_value", concat(lit("Processed: "), col("raw_value")))
                .withColumn("timestamp", current_timestamp());

        // 3. Start console output stream
        StreamingQuery consoleQuery = processedDF.writeStream()
                .outputMode("append")
                .format("console")
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start();

        // 4. Start Cassandra write stream with proper UUID handling
        this.stringCassandraQuery = processedDF.writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    // Generate proper UUID type (not string)
                    Dataset<Row> batchWithIds = batchDF.withColumn("id", expr("uuid()"));

                    batchWithIds.select(
                                    col("id"),
                                    col("raw_value").as("original_text"),
                                    col("processed_value"),
                                    col("timestamp")
                            ).write()
                            .format("org.apache.spark.sql.cassandra")
                            .option("keyspace", "product_ks")
                            .option("table", "processed_strings")
                            // Explicitly specify the UUID type mapping
                            .option("spark.cassandra.output.ignoreNulls", "true")
                            .mode("append")
                            .save();
                })
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();
    }

    public void awaitTermination() throws Exception {
        if (objectCassandraQuery != null) objectCassandraQuery.awaitTermination();
        if (stringCassandraQuery != null) stringCassandraQuery.awaitTermination();
    }

    public void stopAllStreams() throws TimeoutException {
        if (objectCassandraQuery != null) objectCassandraQuery.stop();
        if (stringCassandraQuery != null) stringCassandraQuery.stop();
    }
}