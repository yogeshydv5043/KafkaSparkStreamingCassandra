package org.learn;

import org.apache.spark.sql.SparkSession;
import org.learn.spark.MultiTopicStreamingService;

import java.util.concurrent.TimeoutException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws TimeoutException {
        System.out.println("Kafka Spark Streaming ...");

            SparkSession spark = SparkSession.builder()
                    .appName("MultiTopicStreaming")
                    .master("local[*]")
                    // Ye 2 lines MUST HAI (Connector ke liye)
                    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
                    .config("spark.cassandra.connection.host", "localhost") // Cassandra IP
                    // Agar authentication chahiye
//                    .config("spark.cassandra.auth.username", "your_username")
//                    .config("spark.cassandra.auth.password", "your_password")
                    .getOrCreate();

            MultiTopicStreamingService service = new MultiTopicStreamingService(spark, "localhost:9092");

            try {
                service.startObjectTopicStream();  // Different processing for object-topic
                service.startStringTopicStream(); // Different processing for string-topic
                service.awaitTermination();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                service.stopAllStreams();
                spark.close();
            }


    }
}
