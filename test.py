from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import *

# Configuration de Spark
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Configuration du StreamingContext avec une durée de batch de 1 seconde
ssc = StreamingContext(spark.sparkContext, 1)

# Configuration de Kafka
kafkaParams = {
    "bootstrap.servers": "localhost:9092",
    "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
    "group.id": "users_group",
    "auto.offset.reset": "latest"
}
topics = ["users_profiles"]

# Création du flux de Kafka
kafkaStream = KafkaUtils.createDirectStream(ssc, topics, kafkaParams)

# Traitement des données du flux
lines = kafkaStream \
    .map(lambda x: x[1]) \
    .map(lambda x: json.loads(x)) \
    .map(lambda x: x["results"][0])

# Transformation des données
transformed_data = lines \
    .select(
        col("name.title").alias("title"),
        col("name.first").alias("first_name"),
        col("name.last").alias("last_name"),
        col("location.city").alias("city"),
        col("location.state").alias("state"),
        col("location.country").alias("country"),
        col("email"),
        col("phone"),
        col("nat")
    )

# Écriture des données dans Cassandra
transformed_data.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="user_profiles", keyspace="your_keyspace") \
    .mode("append") \
    .save()

# Démarrage du StreamingContext
ssc.start()
ssc.awaitTermination()

