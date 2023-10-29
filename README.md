# Description
Ce programme de formation vise à habiliter les participants à concevoir, développer et déployer un pipeline de données en temps réel en utilisant PySpark, Kafka, Cassandra et MongoDB. Les participants apprendront à transformer, agréger et stocker efficacement les données utilisateur générées par randomuser.me.

# Contexte du projet
Dans un monde où les données sont considérées comme le nouvel or, il est impératif pour les organisations de pouvoir traiter et analyser les données en temps réel pour prendre des décisions éclairées. Ce programme est conçu pour les professionnels de la donnée qui cherchent à acquérir des compétences pratiques dans la mise en œuvre de pipelines de données en temps réel.
En tant que developpeur Data, le professionnel en charge de cette situation est sollicité pour mettre en place un pipeline pour répondre à ces défis

# Set Up PySpark

### Installer Python 3 et virtualenv
````
sudo apt-get update
sudo apt-get install python3 python3-venv
````
### Créer un environnement virtuel
````
python3 -m venv sparkenv
````

### Activer l'environnement virtuel
````
source sparkenv/bin/activate
````
### Installer PySpark dans l'environnement
````
pip install pyspark
````

# Pull and Run Kafka and Zookeeper docker images
### Method 1: pull_&_run very easily zookeeper et kafka docker images with Conduktor

 ## Executer cette commande pour installer et/ou demarrer zookeeper et kafka avec un seul broker 
```
 docker compose -f ./zk-single-kafka-single.yml up
```

 ### Method 2: installing and running Kafka with ZooKeeper according to the documentation
 Download the latest Kafka release here: https://www.apache.org/dyn/closer.cgi?path=/kafka/3.6.0/kafka_2.13-3.6.0.tgz 
 and extract it:

```
$ tar -xzf kafka_2.13-3.6.0.tgz
$ cd kafka_2.13-3.6.0
```
Run the following commands in order to start all services in the correct order:

# Start the ZooKeeper service
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
Open another terminal session and run:

# Start the Kafka broker service
```
bin/kafka-server-start.sh config/server.properties
```

## Run one of the following commands to install Conduktor
Conduktor complements your existing Kafka infrastructure.

1- Launch Conduktor with a preconfigured Kafka (Redpanda):
 
```
curl -L https://releases.conduktor.io/quick-start -o docker-compose.yml && docker compose up -d --wait && echo "Conduktor started on http://localhost:8080"
```
2- OR Launch Conduktor and connect it to your own Kafka:

```
curl -L https://releases.conduktor.io/console -o docker-compose.yml && docker compose up -d --wait && echo "Conduktor started on http://localhost:8080"
 ```


 
 

