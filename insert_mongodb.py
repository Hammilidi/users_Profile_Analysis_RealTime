from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, concat_ws, col, split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, FloatType
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, CollectionInvalid
import pyspark.sql.functions as F


def save_to_mongodb(iter):
    try:
        client = MongoClient("mongodb://localhost:27017")
        db_name = "users_profiles"
        collection_name = "user_data"

        db = client[db_name]
        collection = db[collection_name]

        data = {
            "identifiant": iter.identifiant,
            "full_name": iter.full_name,
            "domain_name": iter.domain_name,
            "gender": iter.gender,
            "country": iter.country,
            "age": iter.age,
            "username": iter.username,
            "inscription": iter.inscription
        }

        collection.insert_one(data)

    except ConnectionFailure as e:
        print(f"Failed to connect to MongoDB: {e}")

    except CollectionInvalid as e:
        print(f"Failed to create collection: {e}")



spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka to MongoDB") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("local[*]") \
    .getOrCreate()

# Définir un schéma pour les données entrantes

schema = StructType([
    StructField("results", ArrayType(StructType([
        StructField("gender", StringType(), True),
        StructField("name", StructType([
            StructField("title", StringType(), True),
            StructField("first", StringType(), True),
            StructField("last", StringType(), True)
        ]), True),

        StructField("location", StructType([
            StructField("street", StructType([
                StructField("number", StringType(), True),
                StructField("name", StringType(), True)
            ]), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("postcode", StringType(), True),
            StructField("country", StringType(), True),
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
            StructField("value", StringType(), True),
        ]), True),

        StructField("picture", StructType([
            StructField("large", StringType(), True),
            StructField("medium", StringType(), True),
            StructField("thumbnail", StringType(), True),
        ]), True),

        StructField("nat", StringType(), True)
    ]), True))
])


# Lire depuis Kafka
kafkaStream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "users_profiles") \
    .option("startingOffsets", "earliest") \
    .load()

 
# Analyser les messages Kafka et appliquer le schéma
parsed_stream = kafkaStream.selectExpr("CAST(value AS STRING)")
df = parsed_stream.withColumn("values", from_json(parsed_stream["value"], schema)).selectExpr("explode(values.results) as data")


# Extraction des champs à insérer dans mongodb
result_df = df.select(
    F.col("data.login.uuid").alias("identifiant"),
    F.col("data.gender").alias("gender"),
    F.concat_ws(" ", F.col("data.name.last"), F.col("data.name.first")).alias("full_name"),
    F.col("data.login.username").alias("username"),
    F.split(F.col("data.email"), "@").getItem(1).alias("domain_name"),
    F.col("data.location.country"),
    F.round(F.datediff(F.current_date(), F.to_date(F.col("data.dob.date")))/365).alias("age"),
    F.col("data.registered.date").alias("inscription")
)


# Write the data to MongoDB
result_df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.coalesce(1).write.format("json").save(f"output/{batch_id}")) \
    .foreach(save_to_mongodb) \
    .outputMode("append") \
    .start() \
    .awaitTermination()


