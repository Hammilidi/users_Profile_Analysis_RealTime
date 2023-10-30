from confluent_kafka import Consumer, KafkaError


# Définir le nom du serveur Kafka
SERVER_NAME = 'localhost:9092'
# Définir le nom du Topic
TOPIC_NAME = 'users_profiles'
# Configuration du consommateur Kafka
config = {
    'bootstrap.servers': SERVER_NAME,  
    'group.id': 'users_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(config)
consumer.subscribe([TOPIC_NAME])

while True:
    msg = consumer.poll(3.0)
    # print(msg)

    if msg is None:
        continue

    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print("Fin de la partition, continuer la lecture...")
        else:
            print(f"Erreur lors de la lecture du message : {msg.error()}")
    else:
        print(f"Reçu un message : {msg.value()}")
