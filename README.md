# Description
Ce programme de formation vise à habiliter les participants à concevoir, développer et déployer un pipeline de données en temps réel en utilisant PySpark, Kafka, Cassandra et MongoDB. Les participants apprendront à transformer, agréger et stocker efficacement les données utilisateur générées par randomuser.me.

# Contexte du projet
Dans un monde où les données sont considérées comme le nouvel or, il est impératif pour les organisations de pouvoir traiter et analyser les données en temps réel pour prendre des décisions éclairées. Ce programme est conçu pour les professionnels de la donnée qui cherchent à acquérir des compétences pratiques dans la mise en œuvre de pipelines de données en temps réel.
En tant que developpeur Data, le professionnel en charge de cette situation est sollicité pour mettre en place un pipeline pour répondre à ces défis


# Pull and Run Kafka and Zookeeper docker images
### Method 1: pull_&_run very easily zookeeper et kafka docker images with Conduktor

## Executer cette commande pour installer et/ou demarrer zookeeper et kafka avec un seul broker 
```
 docker compose -f ./zk-single-kafka-single.yml up
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

# Create a topic 
1- Move to 
```
cd kafka_2.13-3.6.0
```
2- Run this command to create a topic
```
bin/kafka-topics.sh --create --topic users_profiles --bootstrap-server localhost:9092
```
3- Run this command to show you details such as the partition count of the new topic
```
bin/kafka-topics.sh --describe --topic users_profiles --bootstrap-server localhost:9092
```

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

# installer Apache Spark

### install required packages for spark
```
sudo apt install default-jdk scala git -y
```

### verify the installed dependencies
```
java -version; javac -version; scala -version; git --version
```

### Télécharger apache spark
Accédez au site Web de Apache Spark : https://spark.apache.org/downloads.html
Choisissez la dernière version stable de Spark prebuilt for Apache Hadoop et téléchargez le fichier tgz (par exemple, spark-3.2.0-bin-hadoop3.2.tgz).
1- Téléchargez le fichier
```
wget https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz
Une fois le téléchargement terminé, ouvrez un terminal et accédez au répertoire où vous avez téléchargé Spark.

```
2- Décompressez votre fichier téléchargé
```
tar -xzf spark-3.2.0-bin-hadoop3.2.tgz

```
3- Déplacez spark vers un dossier approprié
```
sudo mv spark-3.2.0-bin-hadoop3.2 /opt/spark

```
4- Configuration des variables d'environnement
Pour que Spark fonctionne correctement, vous devez configurer certaines variables d'environnement. Modifiez le fichier .bashrc en utilisant un éditeur de texte (comme nano) :
```
nano ~/.bashrc

```
Ajoutez les lignes suivantes à la fin du fichier :
```
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin
export PYSPARK_PYTHON=python3

```
Enregistrez le fichier et fermez l'éditeur. Pour prendre en compte les modifications, rechargez les fichiers de configuration en utilisant la commande suivante :
```
source ~/.bashrc

```
5- Vérificqtion de l'installation
Vérification de la version de spark
```
spark-submit --version
```
Accédez à shell pour scala
```
spark-shell
```
Vous devriez voir l'interface Spark Scala REPL s'ouvrir.
Si vous préférez utiliser Python pour interagir avec Spark, vous pouvez utiliser la Spark Shell en Python avec la commande pyspark. Voici comment :
```
pyspark

```
Vous pouvez démarrer un serveur master manuellement
``` 
./sbin/start-master.sh
```
Vous pouvez trouver cette URL sur l'interface utilisateur Web du maître, qui est http://localhost:8080 par défaut.

Quelques commandes utiles
```
sbin/start-master.sh- Démarre une instance maître sur la machine sur laquelle le script est exécuté.
sbin/start-workers.sh- Démarre une instance de travail sur chaque machine spécifiée dans le conf/workersfichier.
sbin/start-worker.sh- Démarre une instance de travail sur la machine sur laquelle le script est exécuté.
sbin/start-connect-server.sh- Démarre un serveur Spark Connect sur la machine sur laquelle le script est exécuté.
sbin/start-all.sh- Démarre à la fois un maître et un certain nombre de travailleurs comme décrit ci-dessus.
sbin/stop-master.sh- Arrête le maître qui a été démarré via le sbin/start-master.shscript.
sbin/stop-worker.sh- Arrête toutes les instances de travail sur la machine sur laquelle le script est exécuté.
sbin/stop-workers.sh- Arrête toutes les instances de travail sur les machines spécifiées dans le conf/workersfichier.
sbin/stop-connect-server.sh- Arrête toutes les instances du serveur Spark Connect sur la machine sur laquelle le script est exécuté.
sbin/stop-all.sh- Arrête à la fois le maître et les ouvriers comme décrit ci-dessus.
```



