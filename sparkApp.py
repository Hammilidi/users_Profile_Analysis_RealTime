import findspark
from pyspark.sql.functions import col, regexp_replace
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
SparkSession.builder.config(conf=SparkConf())
from pyspark.sql.functions import sha2,concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType, FloatType, BinaryType
from pyspark.sql.functions import from_json, explode, concat_ws, when, col, current_date, datediff
from pyspark.sql import DataFrame
import pyspark.sql.functions as F







findspark.init()

# Initialiser une session Spark
spark = SparkSession.builder.appName("RealTimeApp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Lire depuis Kafka
kafkaStream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "users_profiles") \
    .load()
 
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

# Analyser les messages Kafka et appliquer le schéma
parsed_stream = kafkaStream.selectExpr("CAST(value AS STRING)")
df = parsed_stream.withColumn("values", from_json(parsed_stream["value"], schema)).selectExpr("explode(values.results) as data")




# ----------------------------------------------------------TRANSFORMATIONS------------------------------------------

result_df = df.select(
    F.col("data.name.title").alias("title"),
    F.col("data.login.uuid").alias("identifiant"),
    F.col("data.gender").alias("gender"),

    F.concat_ws(" ",
    F.col("data.name.last"),
    F.col("data.name.first")).alias("full_name"),

    F.col("data.login.username").alias("username"),

    F.col("data.email").alias("email"),

    F.split(F.col("data.email"), "@").getItem(1).alias("domain_name"),

    F.col("data.phone").alias("phone"),

    F.concat_ws(", ",
    F.col("data.location.city"),
    F.col("data.location.state"),
    F.col("data.location.country")).alias("full_address"),

    F.round(F.datediff(F.current_date(), F.to_date(F.col("data.dob.date")))/365).alias("age"),
    F.col("data.registered.date").alias("inscription"),
    F.col("data.nat").alias("nationality")
)
# encrypt email,passowrd,phone,cell using SHA-256
result_df = result_df.withColumn("email", F.sha2(result_df["email"], 256))
result_df = result_df.withColumn("phone", F.sha2(result_df["phone"], 256))
result_df = result_df.withColumn("full_address", F.sha2(result_df["full_address"], 256))

# filtrer l'age > 13
result_df = result_df.filter(col("age") > 13)


# -----------------------------------------------------------CASSANDRA----------------------------------------------------
# Connexion avec Cassandra
spark.conf.set("spark.cassandra.connection.host", "localhost")  # Remplacez "localhost" par l'adresse IP de votre nœud Cassandra
spark.conf.set("spark.cassandra.connection.port", "9042")  # Le port par défaut de Cassandra est 9042
print("Connexion à Cassandra établie !")

# Define the keyspace
keyspace = "mykeyspace"

# Define the table name
table = "users_profiles"

# Définir la fonction save_to_cassandra_table
def save_to_cassandra_table(iter):
    iter.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", "./checkpoint") \
        .option("keyspace", keyspace) \
        .option("table", table) \
        .mode("append") \
        .save()

# -----------------------------------------------------------STREAMING----------------------------------------------------
result_df.writeStream \
    .foreach(save_to_cassandra_table) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
    
    