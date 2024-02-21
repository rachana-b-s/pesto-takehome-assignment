from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("AdvertiseX Data Processing") \
    .getOrCreate()

# Initialize Kafka Producer
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Sample CSV schema
csv_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("ad_campaign_id", StringType(), True),
    StructField("conversion_type", StringType(), True),
    StructField("revenue", StringType(), True)  # Assuming revenue is stored as string in the CSV
])

# Error Handling and Monitoring
def log_error(error_message):
    """
    Log error messages to Kafka topic.
    """
    producer.send('error_topic', error_message.encode('utf-8'))

def monitor_metrics(metric_name, value):
    """
    Publish custom metrics to Kafka topic.
    """
    producer.send('metric_topic', (metric_name + ': ' + str(value)).encode('utf-8'))

# Data Ingestion
def ingest_data_from_kafka():
    """
    Ingest ad impressions data from Kafka topic.
    """
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "ad_impressions_topic") \
        .load()
    return kafka_df

def ingest_data_from_csv():
    """
    Ingest clicks/conversions data from CSV files stored in Amazon S3.
    """
    csv_df = spark \
        .read \
        .schema(csv_schema) \
        .csv("s3://path/to/csv/data")
    return csv_df

def ingest_data_from_avro():
    """
    Ingest bid requests data from Avro files stored in Amazon S3.
    """
    avro_df = spark \
        .read \
        .format("avro") \
        .load("s3://path/to/avro/data")
    return avro_df


# Data Processing
def process_json_data(df):
    """
    Perform data processing tasks for JSON data.
    """
    # Clean and handle wrong data: Replace missing ad creative IDs with a default value
    processed_df = df.withColumn("ad_creative_id", when(col("ad_creative_id").isNull(), "default_creative_id").otherwise(col("ad_creative_id")))
    
    return processed_df

def process_csv_data(df):
    """
    Perform data processing tasks for CSV data.
    """
    # Clean and handle wrong data: Convert revenue column to numeric type and handle non-numeric values
    processed_df = df.withColumn("revenue", df["revenue"].cast("float"))
    return processed_df

def process_avro_data(df):
    """
    Perform data processing tasks for Avro data.
    """
    # Clean and handle wrong data: Replace missing user IDs with a default value
    processed_df = df.withColumn("user_id", when(col("user_id").isNull(), "default_user_id").otherwise(col("user_id")))
    return processed_df

# Main function
if __name__ == "__main__":
    # Ingest data from different sources
    ad_impressions_df = ingest_data_from_kafka()
    clicks_conversions_df = ingest_data_from_csv()
    bid_requests_df = ingest_data_from_avro()

    # Process data
    processed_ad_impressions_df = process_json_data(ad_impressions_df)
    processed_clicks_conversions_df = process_csv_data(clicks_conversions_df)
    processed_bid_requests_df = process_avro_data(bid_requests_df)

    # Start streaming query for Kafka ingestion
    kafka_query = processed_ad_impressions_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # Error Handling and Monitoring
    try:
        kafka_query.awaitTermination()
    except Exception as e:
        log_error("An error occurred in the data processing pipeline: " + str(e))
        raise e
    finally:
        # Publish monitoring metrics
        monitor_metrics("processing_status", "completed")
