from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

def save_to_cassandra_table(dataframe: DataFrame, keyspace: str, table: str):
    # Sauvegarde des données dans une table Cassandra
    dataframe.write \
             .format("org.apache.spark.sql.cassandra") \
             .options(table=table, keyspace=keyspace) \
             .mode("append") \
             .save()

def save_to_mongodb_collection(dataframe: DataFrame, collection: str):
    # Sauvegarde des données dans une collection MongoDB
    dataframe.write \
             .format("mongo") \
             .option("uri", "mongodb://localhost:27017") \
             .option("database", "mydb") \
             .option("collection", collection) \
             .mode("append") \
             .save()


# Initialiser une session spark
spark = SparkSession.builder \
    .appName("Mon Application") \
    .config("spark.jars", "file:///D://spark_dependency_jars/kafka-client.jar,file:///D://spark_dependency_jars/cassandra-driver.jar,file:///D://spark_dependency_jars/mongo-driver.jar") \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()

# Lire depuis kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_profiles") \
    .load()

# ... Transformations 


# Ecrire dans Cassandra les donnees transformees 
yourDataFrame.writeStream
  .format("org.apache.spark.sql.cassandra")
  .option("table", "your_cassandra_table")
  .option("keyspace", "your_keyspace")
  .start()

# Aggregations avec Spark



# Stockage des donnees agreges dans MongoDB avec la bibliotheque MongoDB Spark Connector
yourAggregatedDataFrame.writeStream
  .format("com.mongodb.spark.sql.DefaultSource")
  .option("uri", "mongodb://localhost:27017/yourdb.your_collection")
  .start()



