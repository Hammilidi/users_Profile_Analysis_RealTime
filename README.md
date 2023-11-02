# Description
Ce projet vise à habiliter les participants à concevoir, développer et déployer un pipeline de données en temps réel en utilisant PySpark, Kafka, Cassandra et MongoDB. Les participants apprendront à transformer, agréger et stocker efficacement les données utilisateur générées par randomuser.me.

# Contexte du projet
Dans un monde où les données sont considérées comme le nouvel or, il est impératif pour les organisations de pouvoir traiter et analyser les données en temps réel pour prendre des décisions éclairées. Ce programme est conçu pour les professionnels de la donnée qui cherchent à acquérir des compétences pratiques dans la mise en œuvre de pipelines de données en temps réel.
En tant que developpeur Data, le professionnel en charge de cette situation est sollicité pour mettre en place un pipeline pour répondre à ces défis

# Mise en place de l'environnemet de travail

## Set up Kafka and Zookeeper 
#### Method 1: pull_&_run very easily zookeeper et kafka docker images with Conduktor

Executer cette commande pour installer et/ou demarrer zookeeper et kafka avec un seul broker 
```
 docker compose -f ./zk-single-kafka-single.yml up
```

## Run one of the following commands to install Conduktor (Kafka GUI)
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

1. Start the ZooKeeper service
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
Open another terminal session and run:

2. Start the Kafka broker service
```
bin/kafka-server-start.sh config/server.properties
```

3. Create a topic 
a. Move to 
```
cd kafka_2.13-3.6.0
```
b. Run this command to create a topic
```
bin/kafka-topics.sh --create --topic users_profiles --bootstrap-server localhost:9092
```
c. Run this command to show you details such as the partition count of the new topic
```
bin/kafka-topics.sh --describe --topic users_profiles --bootstrap-server localhost:9092
```

#### Set Up PySpark

1. Installer Python 3 et virtualenv
````
sudo apt-get update
sudo apt-get install python3 python3-venv
````
2. Créer un environnement virtuel
````
python3 -m venv sparkenv
````

3. Activer l'environnement virtuel
````
source sparkenv/bin/activate
````
4. Installer PySpark dans l'environnement
````
pip install pyspark
````

## installer Apache Spark

1. install required packages for spark
```
sudo apt install default-jdk scala git -y
```

2. verify the installed dependencies
```
java -version; javac -version; scala -version; git --version
```

3. Télécharger apache spark
Accédez au site Web de Apache Spark : https://spark.apache.org/downloads.html
Choisissez la dernière version stable de Spark prebuilt for Apache Hadoop et téléchargez le fichier tgz (par exemple, spark-3.2.0-bin-hadoop3.2.tgz).
a. Téléchargez le fichier
```
wget https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz
Une fois le téléchargement terminé, ouvrez un terminal et accédez au répertoire où vous avez téléchargé Spark.

```
b. Décompressez votre fichier téléchargé
```
tar -xzf spark-3.2.0-bin-hadoop3.2.tgz

```
c. Déplacez spark vers un dossier approprié
```
sudo mv spark-3.2.0-bin-hadoop3.2 /opt/spark

```
d. Configuration des variables d'environnement
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
e. Vérification de l'installation
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
## Install Apache Cassandra with Docker
```
docker pull cassandra:latest         ---pour telecharger la derniere version de l'image
docker network create cassandra      ---pour l'acces au reseau de docker
docker run --rm -d --name cassandra --hostname cassandra --network cassandra cassandra         ---pour demarrer cassandra
docker exec -it cassandra cqlsh     ---pour entrer dans le shell cqlsh de cassandra

```

## Install mongoDB with Docker

```
docker run --name mongodb -d -p 27017:27017 mongo
docker stop mongodb

```

# Schéma des données de l'API
Les données de l'API randomuser.me sont structurées comme suit :

1. Gender : Une chaîne indiquant le sexe de l'individu.
2. Name :
        Titre : Une chaîne représentant le titre de la personne.
        First Name : Une chaîne représentant le prénom de la personne.
        Last Name : Une chaîne représentant le nom de famille de la personne.
3. Localisation :
        street :
             Number : Un entier représentant le numéro de la rue.
             Name : Une chaîne représentant le nom de la rue.
        City : Une chaîne représentant la ville.
        State : Une chaîne représentant l’état.
        Country : Une chaîne représentant le pays.
        Postcode : Une chaîne représentant le code postal.
        Coordinates :
             Latitude : Une chaîne représentant la latitude.
             Longitude : Une chaîne représentant la longitude.
        Timezone :
             Offset : Une chaîne représentant le décalage UTC.
             Description : Une chaîne décrivant le fuseau horaire.
4. Email : Une chaîne représentant l'adresse e-mail.
5. Login :
        UUID : une chaîne représentant l'identifiant universellement unique.
        Username : Une chaîne représentant le nom d'utilisateur.
        Password: Une chaîne représentant le mot de passe.
        Salt : Une chaîne utilisée dans le hachage du mot de passe.
        MD5 : Une chaîne représentant le hachage MD5.
        SHA1 : Une chaîne représentant le hachage SHA1.
        SHA256 : Une chaîne représentant le hachage SHA256.
6. Date Of Birth (DOB) :
        Date : Une chaîne représentant la date de naissance.
        Age : Un entier représentant l'âge.
7. Registered :
        Date : Une chaîne représentant la date d'inscription.
        Age : Un entier représentant l'âge d'inscription.
8. Phone : Une chaîne représentant le numéro de téléphone.
9. Cell : Une chaîne représentant le numéro de téléphone portable.
10. ID :
        Name : Une chaîne représentant le nom de l'identifiant.
        Value : Une chaîne représentant la valeur de l'identifiant.
11. Picture URLs :
        Large : Une chaîne représentant l'URL de la grande image.
        Medium : Une chaîne représentant l'URL de l'image de taille moyenne.
        Thumbnail : une chaîne représentant l'URL de l'image miniature.
12. Nationality (Nat) : Une chaîne représentant la nationalité.


# La logique du Producer

# Les transformations effectuées

