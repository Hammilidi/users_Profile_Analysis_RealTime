
from pymongo import MongoClient

# MongoDB connection settings
mongo_host = "localhost" # Hostname of the MongoDB server (if running in a Docker container)
mongo_port = 27017 # Port number

# Database and collection names
db_name = "userdbd" # Replace with your database name
collection_name = "userco11" # Replace with your collection name

# Connect to MongoDB
client = MongoClient(mongo_host, mongo_port)

# Access the database
db = client[db_name]

# Access the collection
collection = db[collection_name]

# Get the first 10 documents from the collection
query_result = collection.find().limit(10)

# Print the documents
for document in query_result:
    print(document)

# close the connection
client.close()
