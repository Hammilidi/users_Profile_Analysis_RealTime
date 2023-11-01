import findspark
findspark.init()
from pyspark.sql.functions import col, regexp_replace
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
SparkSession.builder.config(conf=SparkConf())
from pyspark.sql.functions import sha2,concat_ws
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType,BinaryType
from pyspark.sql.functions import from_json,explode
from pyspark.sql.functions import when, col, datediff, current_date



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
    
    StructField("nat", StringType(), True),
])


# Analyser les messages Kafka et appliquer le schéma
parsed_stream = kafkaStream.selectExpr("CAST(value AS STRING)")
selected_df = parsed_stream.withColumn("values", from_json(col("value"), schema)).select("values.*")

# Afficher le flux de données en streaming
query = selected_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Attendre la fin de la requête (peut être stoppée manuellement)
query.awaitTermination()


# Transformation des Données
# full name
transformed_stream = selected_df.withColumn("full_name", concat_ws(" ", col("name.first"), col("name.last")))

# full location 
transformed_stream = transformed_stream.withColumn("location_full", concat_ws(", ",
    col("location.street.number"), col("location.street.name"),
    col("location.city"), col("location.state"), col("location.postcode"),
    col("location.country")))

# Valider ou recalculer l'âge
transformed_stream = transformed_stream.withColumn("age",
    when(col("dob.age") < 0, datediff(current_date(), col("dob.date")) / 365).otherwise(col("dob.age"))
)

# Afficher le flux de données transformées
query = transformed_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Attendre la fin de la requête (peut être stoppée manuellement)
query.awaitTermination()


# Créez un flux Spark et définissez les opérations de sortie
query = kafkaStream \
    .writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "your_keyspace") \
    .option("table", "your_table") \
    .start()

# # Attendez la fin du streaming
# query.awaitTermination()