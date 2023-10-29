import requests
import json
import gzip
from confluent_kafka import Producer

# Configuration du producteur Kafka
producer = Producer({'bootstrap.servers': 'kafka1:19092'})

# Récupérer les données depuis randomuser.me
url = "https://randomuser.me/api/?results=5000"
response = requests.get(url)
data = response.json()

# Sérialiser en JSON
json_data = json.dumps(data).encode('utf-8')

# Compresser les données JSON
compressed_data = gzip.compress(json_data)

# Envoyer les données compressées au sujet Kafka
producer.produce('randomuser_data_topic', key=None, value=compressed_data)
producer.flush()
