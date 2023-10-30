from confluent_kafka import Producer, KafkaError
import requests
import json

# Définir le nom du serveur Kafka
SERVER_NAME = 'localhost:9092'
# Définir le nom du Topic
TOPIC_NAME = 'users_profiles'

# Configuration du producteur Kafka
config = {'bootstrap.servers': SERVER_NAME}

producer = Producer(config)

def delivery_report(err, msg):
    if err is not None:
        print('Erreur lors de l\'envoi de message : {}'.format(err))
    else:
        print('Message envoyé avec succès à {}, offset : {}'.format(msg.topic(), msg.offset()))

try:
    # Récupérer les données depuis randomuser.me
    url = "https://randomuser.me/api/"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Erreur HTTP : Code de statut {response.status_code}")
    else:
        data = response.json()
        print(data["results"][0])

        # Sérialiser en JSON
        json_data = json.dumps(data).encode('utf-8')

        # Envoyer les données au sujet Kafka
        producer.produce(TOPIC_NAME, value=json_data, callback=delivery_report)
        producer.flush()

        print("Données envoyées avec succès à Kafka")

except requests.exceptions.RequestException as req_error:
    print("Erreur lors de la demande vers randomuser.me:", req_error)

except Exception as e:
    print("Une erreur s'est produite:", e)
