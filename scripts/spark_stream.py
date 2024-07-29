# from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampNTZType, DoubleType, LongType
from utils.minio_utils import MinIOClient
from utils.helpers import load_cfg
import logging
import warnings
import json


logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
            
warnings.filterwarnings('ignore')

CFG_FILE_SPARK = "./config/spark.yaml"
cfg = load_cfg(CFG_FILE_SPARK)
spark_cfg = cfg["spark_config"]
MEMORY = spark_cfg['executor_memory']

def consume_kafka_topic(endpoint_url, access_key, secret_key):

    
    topic = "device.iot.taxi_nyc_time_series"
    
    print("Consuming Kafka Topic: {}".format(topic))


    client = MinIOClient(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key
    )
    
    # Create bucket 'bronze' in MinIO
    # BUCKET_STREAM = "raw-stream"
    client.create_bucket("bronze")
    
    
    # # Create bucket 'spark-checkpoint' in MinIO
    # BUCKET_SPARK_CHECKPOINT = "spark-checkpoint"
    # client.create_bucket(BUCKET_SPARK_CHECKPOINT)
    
    from pyspark.sql import SparkSession
    from delta.pip_utils import configure_spark_with_delta_pip

    schema = StructType([
        StructField("vendorid", IntegerType(), True),
        StructField("tpep_pickup_datetime", LongType(), True),
        StructField("tpep_dropoff_datetime", LongType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("ratecodeid", DoubleType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("pulocationid", IntegerType(), True),
        StructField("dolocationid", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
    ])


    spark_session = (SparkSession.builder.config("spark.executor.memory", MEMORY) \
                    .config(
                        "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,org.apache.hadoop:hadoop-aws:2.8.2"
                    )
                    .config("spark.hadoop.fs.s3a.access.key", access_key)
                    .config("spark.hadoop.fs.s3a.secret.key", secret_key)
                    .config("spark.hadoop.fs.s3a.endpoint", endpoint_url)
                    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                    .config("spark.hadoop.fs.s3a.path.style.access", "true")
                    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                    .appName("Kafka to deltalake")
                    .getOrCreate()
    )
    
    logging.info('Spark session successfully created!')

    df = (spark_session
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", "streaming-broker:29092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load())


    # # Load the configuration file
    # with open('../stream_', 'r') as f:
    #     config = json.load(f)
    
    json_string = """
    {
    "fields": [
        { "name": "dolocationid", "type": "IntegerType", "nullable": true },
        { "name": "pulocationid", "type": "IntegerType", "nullable": true },
        { "name": "ratecodeid", "type": "DoubleType", "nullable": true },
        { "name": "vendorid", "type": "IntegerType", "nullable": true },
        { "name": "congestion_surcharge", "type": "DoubleType", "nullable": true },
        { "name": "extra", "type": "DoubleType", "nullable": true },
        { "name": "fare_amount", "type": "DoubleType", "nullable": true },
        { "name": "improvement_surcharge", "type": "DoubleType", "nullable": true },
        { "name": "mta_tax", "type": "DoubleType", "nullable": true },
        { "name": "passenger_count", "type": "DoubleType", "nullable": true },
        { "name": "payment_type", "type": "IntegerType", "nullable": true },
        { "name": "tip_amount", "type": "DoubleType", "nullable": true },
        { "name": "tolls_amount", "type": "DoubleType", "nullable": true },
        { "name": "total_amount", "type": "DoubleType", "nullable": true },
        { "name": "tpep_dropoff_datetime", "type": "LongType", "nullable": true },
        { "name": "tpep_pickup_datetime", "type": "LongType", "nullable": true },
        { "name": "trip_distance", "type": "DoubleType", "nullable": true }
    ]
    }
    """

    config = json.loads(json_string)

    # Define a mapping from type names to PySpark types
    type_mapping = {
        "IntegerType": IntegerType(),
        "StringType": StringType(),
        "TimestampNTZType": TimestampNTZType(),
        "DoubleType": DoubleType(),
        "LongType": LongType()
    }

    # Create the schema based on the configuration file
    payload_after_schema = StructType([
        StructField(field["name"], type_mapping[field["type"]], field["nullable"])
        for field in config["fields"]
    ])

    data_schema = StructType([
        StructField("payload", StructType([
            StructField("after", payload_after_schema, True)
        ]), True)
    ])

    parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
                .select(from_json(col("json"), data_schema).alias("data")) \
                .select("data.payload.after.*")

    parsed_df = parsed_df \
        .withColumn("tpep_pickup_datetime", (col("tpep_pickup_datetime") / 1000000).cast("timestamp")) \
        .withColumn("tpep_dropoff_datetime", (col("tpep_dropoff_datetime") / 1000000).cast("timestamp"))

    parsed_df.createOrReplaceTempView("nyc_taxi_view")

    df_final = spark_session.sql("""
        SELECT
            * 
            , date_format(current_date(), 'yyyyMMdd') AS p_date
        FROM nyc_taxi_view
    """)

    print(df_final.printSchema())

    
    path_write  = "s3a://bronze/fake_device_iot_taxi_nyc_time_series/"
    
    # df_final.write.format("delta").partitionBy("p_date").mode("overwrite").save(path_write)
    df_final.write.partitionBy("p_date").format("parquet").save(path_write) 
    df_final.show(10)

    logging.info("Batch job completed.")
    logging.info(f"kafka topic `{topic}` Data written to Delta Lake at path: {path_write}")
