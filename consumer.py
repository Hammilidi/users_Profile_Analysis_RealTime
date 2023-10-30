from confluent_kafka import Consumer, KafkaError

# Configuration du consommateur Kafka
config = {
    'bootstrap.servers': 'your_kafka_broker',  # Remplacez par votre broker Kafka
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(config)
consumer.subscribe(['randomuser_data_topic'])

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print("Fin de la partition, continuer la lecture...")
        else:
            print(f"Erreur lors de la lecture du message : {msg.error()}")
    else:
        print(f"Reçu un message : {msg.value()}")
