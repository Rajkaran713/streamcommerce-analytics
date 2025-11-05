"""
Spark Structured Streaming: Consume and process Kafka events
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

class StreamingConsumer:
    """Real-time event processing with Spark Structured Streaming"""
    
    def __init__(self):
        # Create Spark session
        self.spark = SparkSession.builder \
            .appName("StreamCommerce-Clickstream") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
            .master("local[*]") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        print("âœ… Spark session created")
        print(f"   Spark version: {self.spark.version}")
    
    def read_clickstream(self):
        """Read clickstream events from Kafka"""
        
        # Define schema for clickstream events
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("device", StringType(), True),
            StructField("page_url", StringType(), True),
            StructField("referrer", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity", IntegerType(), True)
        ])
        
        # Read from Kafka (use kafka:9093 for Docker internal network)
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9093") \
            .option("subscribe", "clickstream_events") \
            .option("startingOffsets", "earliest") \
            .load()
        
        # Parse JSON
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        return parsed_df
    
    def aggregate_events(self, df):
        """Aggregate events in 30-second windows"""
        
        # Convert timestamp to proper format
        df = df.withColumn("event_time", to_timestamp(col("timestamp")))
        
        # Aggregate by event type in 30-second windows
        # Use approx_count_distinct instead of countDistinct for streaming
        aggregated = df \
            .withWatermark("event_time", "1 minute") \
            .groupBy(
                window(col("event_time"), "30 seconds"),
                col("event_type")
            ) \
            .agg(
                count("*").alias("event_count"),
                approx_count_distinct("customer_id").alias("unique_customers"),
                approx_count_distinct("product_id").alias("unique_products")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("event_type"),
                col("event_count"),
                col("unique_customers"),
                col("unique_products")
            )
        
        return aggregated
    
    def start_console_output(self, df, query_name="clickstream_aggregation"):
        """Start streaming query with console output"""
        
        query = df \
            .writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", False) \
            .queryName(query_name) \
            .start()
        
        return query
    
    def run_real_time_analysis(self, duration_seconds=60):
        """Run real-time clickstream analysis"""
        
        print("\n" + "="*80)
        print("ðŸ”¥ STARTING REAL-TIME STREAM PROCESSING")
        print("="*80)
        print(f"Duration: {duration_seconds} seconds")
        print("Reading from Kafka topic: clickstream_events")
        print("Processing with 30-second tumbling windows")
        print("\nPress Ctrl+C to stop\n")
        
        try:
            # Read stream
            clickstream = self.read_clickstream()
            
            # Aggregate
            aggregated = self.aggregate_events(clickstream)
            
            # Start output
            query = self.start_console_output(aggregated)
            
            # Wait for specified duration or Ctrl+C
            import time
            start = time.time()
            
            while time.time() - start < duration_seconds:
                time.sleep(5)
                if not query.isActive:
                    break
            
            # Stop query
            query.stop()
            
            print("\nâœ… Stream processing completed")
            
        except KeyboardInterrupt:
            print("\nâš ï¸  Interrupted by user")
        
        except Exception as e:
            print(f"\nâŒ Error: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            self.spark.stop()
            print("âœ… Spark session stopped")
    
    def show_current_metrics(self):
        """Show current metrics from Kafka (batch mode for quick check)"""
        
        print("\n" + "="*80)
        print("ðŸ“Š CURRENT KAFKA METRICS (Batch Mode)")
        print("="*80)
        
        try:
            # Read all available data from Kafka (use kafka:9093 for Docker)
            df = self.spark \
                .read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:9093") \
                .option("subscribe", "clickstream_events") \
                .option("startingOffsets", "earliest") \
                .option("endingOffsets", "latest") \
                .load()
            
            # Define schema
            schema = StructType([
                StructField("event_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("timestamp", StringType(), True),
                StructField("session_id", StringType(), True),
                StructField("device", StringType(), True),
                StructField("page_url", StringType(), True),
                StructField("referrer", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("quantity", IntegerType(), True)
            ])
            
            # Parse JSON
            parsed = df.select(
                from_json(col("value").cast("string"), schema).alias("data")
            ).select("data.*")
            
            total_events = parsed.count()
            
            if total_events == 0:
                print("âš ï¸  No events found in Kafka")
                return
            
            print(f"\nðŸ“ˆ Total Events in Kafka: {total_events:,}")
            
            # Event type breakdown
            print("\nðŸ“Š Event Type Breakdown:")
            parsed.groupBy("event_type") \
                .count() \
                .orderBy(desc("count")) \
                .show()
            
            # Device breakdown
            print("ðŸ“± Device Breakdown:")
            parsed.groupBy("device") \
                .count() \
                .orderBy(desc("count")) \
                .show()
            
            # Top referrers
            print("ðŸ”— Top Referrers:")
            parsed.groupBy("referrer") \
                .count() \
                .orderBy(desc("count")) \
                .show()
            
            # Sample events
            print("ðŸ“‹ Sample Events:")
            parsed.select("event_type", "device", "referrer", "timestamp") \
                .show(10, truncate=False)
            
        except Exception as e:
            print(f"âŒ Error reading from Kafka: {e}")
        
        finally:
            self.spark.stop()


if __name__ == "__main__":
    import sys
    
    consumer = StreamingConsumer()
    
    if len(sys.argv) > 1 and sys.argv[1] == "--metrics":
        # Show current metrics (batch mode)
        consumer.show_current_metrics()
    else:
        # Run real-time streaming (default)
        consumer.run_real_time_analysis(duration_seconds=60)