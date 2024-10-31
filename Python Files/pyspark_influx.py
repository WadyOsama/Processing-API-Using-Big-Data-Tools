import kafka
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from io import StringIO

# Initialize Spark session with Kafka support
spark = SparkSession.builder \
    .appName("Kafka-PySpark-Integration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
    .getOrCreate()

# InfluxDB Configuration
influxdb_url = "https://<your-influxdb-url>/api/v2/write?org=<your-org-id>&bucket=<your-bucket-name>&precision=<precision>"
influxdb_token = "<your-influxdb-token>"

# Define Kafka Variables
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "user_logs"

# Initialize a set to track processed records
processed_records = set()

# Structure for JSON files
schema = StructType([
    StructField("results", ArrayType(
        StructType([
            StructField("gender", StringType(), True),
            StructField("name", StructType([
                StructField("title", StringType(), True),
                StructField("first", StringType(), True),
                StructField("last", StringType(), True)
            ]), True),
            StructField("location", StructType([
                StructField("street", StructType([
                    StructField("number", IntegerType(), True),
                    StructField("name", StringType(), True)
                ]), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("country", StringType(), True),
                StructField("postcode", IntegerType(), True),
                StructField("coordinates", StructType([
                    StructField("latitude", StringType(), True),
                    StructField("longitude", StringType(), True)
                ]), True),
                StructField("timezone", StructType([
                    StructField("offset", StringType(), True),
                    StructField("description", StringType(), True)
                ]), True)
            ]), True),
            StructField("email", StringType(), True),
            StructField("login", StructType([
                StructField("uuid", StringType(), True),
                StructField("username", StringType(), True),
                StructField("password", StringType(), True),
                StructField("salt", StringType(), True),
                StructField("md5", StringType(), True),
                StructField("sha1", StringType(), True),
                StructField("sha256", StringType(), True)
            ]), True),
            StructField("dob", StructType([
                StructField("date", StringType(), True),
                StructField("age", IntegerType(), True)
            ]), True),
            StructField("registered", StructType([
                StructField("date", StringType(), True),
                StructField("age", IntegerType(), True)
            ]), True),
            StructField("phone", StringType(), True),
            StructField("cell", StringType(), True),
            StructField("id", StructType([
                StructField("name", StringType(), True),
                StructField("value", StringType(), True)
            ]), True),
            StructField("picture", StructType([
                StructField("large", StringType(), True),
                StructField("medium", StringType(), True),
                StructField("thumbnail", StringType(), True)
            ]), True),
            StructField("nat", StringType(), True)
        ])
    ), True),
    StructField("info", StructType([
        StructField("seed", StringType(), True),
        StructField("results", IntegerType(), True),
        StructField("page", IntegerType(), True),
        StructField("version", StringType(), True)
    ]), True)
])

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Casting the binary data to string
value_df = df.selectExpr("CAST(value AS STRING)")

# Make it structured with the defined schema
structured_df = value_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Flatten the file
flattened_df = structured_df.select(
    col("results")
).withColumn("result", explode(col("results")))  # Flatten the results array

# Extract individual fields from the exploded result
final_df = flattened_df.select(
    col("result.name.title"),
    col("result.name.first"),
    col("result.name.last"),
    col("result.gender"),
    col("result.email"),
    col("result.location.street"),
    col("result.location.city"),
    col("result.location.state"),
    col("result.location.country"),
    col("result.location.postcode"),
    col("result.location.coordinates.latitude"),
    col("result.location.coordinates.longitude"),
    col("result.dob.age"),
    col("result.phone")
)

# Function sending the data to InfluxDB
def write_to_influxdb(data):
    
    # Use email as a unique identifier
    unique_id = data.get('email')  
    if unique_id in processed_records:
        return  # Skip if already processed
    
    # After successfully writing, add the unique ID to the processed set
    processed_records.add(unique_id)
    
    # Measurement name
    measurement = "users"

    # Prepare tag sets
    tags = []
    if data.get('title'):
        tags.append(f"title={data['title']}")
    if data.get('gender'):
        tags.append(f"gender={data['gender']}")
    if data.get('email'):
        tags.append(f"email={data['email']}")
    
    tags_str = ','.join(tags)

    # Prepare field sets
    fields = []
    if data.get('first'):
        fields.append(f"first=\"{data['first']}\"")
    if data.get('last'):
        fields.append(f"last=\"{data['last']}\"")
    if data.get('street'):
        fields.append(f"street=\"{data['street']}\"")
    if data.get('city'):
        fields.append(f"city=\"{data['city']}\"")
    if data.get('state'):
        fields.append(f"state=\"{data['state']}\"")
    if data.get('country'):
        fields.append(f"country=\"{data['country']}\"")
    if data.get('postcode'):
        fields.append(f"postcode=\"{data['postcode']}\"")
    if data.get('latitude'):
        fields.append(f"latitude=\"{data['latitude']}\"")
    if data.get('longitude'):
        fields.append(f"longitude=\"{data['longitude']}\"")
    if data.get('age') is not None:  # Check for None instead of empty string
        fields.append(f"age={data['age']}")
    if data.get('phone'):
        fields.append(f"phone=\"{data['phone']}\"")

    fields_str = ','.join(fields)

    # Combine into line protocol format
    line_protocol_data = f"{measurement},{tags_str} {fields_str}"

    # Prepare headers
    headers = {
        "Authorization": f"Token {influxdb_token}",
        "Content-Type": "text/plain; charset=utf-8",
    }

    # Send the line protocol data to InfluxDB
    response = requests.post(influxdb_url, data=line_protocol_data, headers=headers)
    
    # Tracking the request result
    if response.status_code != 204:
        print(f"Error writing to InfluxDB: {response.text}")
    else:
        print('Done!')

# Function to process every batch of data
def process_batch(batch_df, batch_id):
    for row in batch_df.collect():
        # Convert row to dictionary
        data = row.asDict()
        # Write each record to InfluxDB
        write_to_influxdb(data)

# Process the streaming data
query = final_df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

# Await termination
query.awaitTermination()

# Stop the Spark session
spark.stop()
