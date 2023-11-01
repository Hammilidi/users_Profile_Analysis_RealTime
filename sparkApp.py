from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import findspark
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
        StructField("first", StringType(), True),
        StructField("last", StringType(), True)
    ]), True),
    StructField("dob", StructType([
        StructField("date", StringType(), True),
        StructField("age", IntegerType(), True)
    ]), True),
    StructField("location", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postcode", StringType(), True)
    ]), True)
])

# # Analyser les messages Kafka et appliquer le schéma
# parsed_stream = kafkaStream.selectExpr("CAST(value AS STRING) as raw_data")
# parsed_stream = parsed_stream.selectExpr("from_json(raw_data, 'your_json_schema') as json_data")
# parsed_stream = parsed_stream.select("json_data.*")

# # Effectuer des transformations
# transformed_stream = parsed_stream.withColumn("full_name", concat_ws(" ", col("name.first"), col("name.last")))
# transformed_stream = transformed_stream.withColumn("location", concat_ws(", ", col("location.street"), col("location.city"), col("location.state"), col("location.postcode")))
# transformed_stream = transformed_stream.withColumnRenamed("dob.age", "age")

# # Afficher le flux de données transformées
# query = transformed_stream.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# # Attendre la fin du streaming
# query.awaitTermination()

# # Créez un flux Spark et définissez les opérations de sortie
# query = kafkaStream \
#     .writeStream \
#     .outputMode("append") \
#     .format("org.apache.spark.sql.cassandra") \
#     .option("keyspace", "your_keyspace") \
#     .option("table", "your_table") \
#     .start()

# # Attendez la fin du streaming
# query.awaitTermination()