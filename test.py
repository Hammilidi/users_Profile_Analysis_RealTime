#-----------------------------------------------------------CASSANDRA----------------------------------------------------
# Connexionn avec Cassandra
spark.conf.set("spark.cassandra.connection.host", "localhost")  # Remplacez "localhost" par l'adresse IP de votre nœud Cassandra
spark.conf.set("spark.cassandra.connection.port", "9042")  # Le port par défaut de Cassandra est 9042
print("connexion à cassanda établie !")

# Define the keyspace
keyspace = "mykeyspace"

# Define the table name
table = "users_profiles"

# Definir la fonction save_to_cassandra_table
def save_to_cassandra_table(dataframe=result_df, keyspace="mykespace", table="users_profiles"):
    dataframe.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace=keyspace, table=table) \
        .mode("append") \
        .save()
        
# Utilisation de la fonction pour insérer les valeurs
save_to_cassandra_table(result_df, keyspace, table)


# Create the keyspace if it doesn't exist
spark.sql(f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")

# Créez la table Cassandra
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {keyspace}.{table} (
        full_name STRING, 
        full_address STRING, 
        age INT,
        hashed_password STRING,
        hashed_dob_date STRING,
        hashed_phone STRING,
        hashed_cell STRING,
        PRIMARY KEY (full_name)
    )
""")

# Sélectionner uniquement les colonnes à insérer
columns_for_cassandra = ["full_name", "age", "full_address", "hashed_password", "hashed_dob_date", "hashed_phone", "hashed_cell"]
selected_columns_df = selected_df.select(columns_for_cassandra)

save_to_cassandra_table()





# ---------------------------------------------------------MongoDB----------------------------------------------------------
import pymongo

# Définissez votre URI de connexion MongoDB
mongo_uri = "mongodb://localhost:27017/"

# Créez une connexion MongoDB
client = pymongo.MongoClient(mongo_uri)

# Sélectionnez votre base de données et votre collection
db = client["mydb"]
collection = db["mycollection"]

# Définissez une fonction pour insérer des données dans MongoDB
def insert_into_mongodb(iterator):
    for record in iterator:
        data_to_mongo = {
            "gender": record.gender,
            "full_name": record.full_name,
            "age": record.age,
            "email": record.email,
            "nat": record.nat
        }
        collection.insert_one(data_to_mongo)

# Utilisez la méthode foreach pour insérer les données dans MongoDB
selected_df.writeStream \
    .foreach(insert_into_mongodb) \
    .start()

