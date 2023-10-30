import requests
import json
import gzip
from confluent_kafka import Producer

# Configuration du producteur Kafka
producer = Producer({'bootstrap.servers': 'kafka1:19092'})

try:
    # Récupérer les données depuis randomuser.me
    url = "https://randomuser.me/api/"
    response = requests.get(url)
    response.raise_for_status()  # Vérifier les erreurs HTTP

    data = response.json()

    # Sérialiser en JSON
    json_data = json.dumps(data["results"][0]).encode('utf-8')

    # Compresser les données JSON
    compressed_data = gzip.compress(json_data)

    # Envoyer les données compressées au sujet Kafka
    producer.produce('randomuser_data_topic', key=None, value=compressed_data)
    producer.flush()

    print("Données envoyées avec succès à Kafka")

except requests.exceptions.RequestException as req_error:
    print("Erreur lors de la demande vers randomuser.me:", req_error)

except Exception as e:
    print("Une erreur s'est produite:", e)
