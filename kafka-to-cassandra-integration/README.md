## Kafka and Cassandra: Integration guide

Build the python docker image to run the producer script
```sh
docker build --tag python:3.10-script-executing-image .
```
Prerequisites to compose the kafka and kafka-ui images
```sh
cd kafka-to-cassandra-integration
# Root must be the owner of the kafka folder 
sudo mkdir -p kafka
sudo chmod -R g+rwX kafka
mkdir -p kafka-ui
touch kafka-ui/config.yml
```

Add the following code to kafka-ui/config.yml:
```sh
kafka:
 clusters:
   - bootstrapServers: kafka:9092
     name: kafka
```
Run the kafka and kafka-ui images
```sh
docker compose up kafka kafka-ui
```
In another terminal, run the producer image. Stop the image using the binding Ctrl + C.
```sh
chmod u+X delete-kafka-topic.sh
delete-kafka-topic.sh # Deletes the kafka topic first
docker compose up python # Create the kafka topic and produce messages into it
```
Create the cassandra server-cqlsh docker container network
```sh
docker network create cassandra-network
```
<!-- Terminal 1: Create the kafka topic and run the producer.py
```sh
docker exec -it kafka bash
kafka-topics.sh --create --topic events-topic --bootstrap-server kafka:9092
python producer.py
``` -->
In another terminal
```sh
docker exec cqlsh cqlsh cassandra 9042 --cqlversion='3.4.5'
```
<!-- Terminal 2: -->
 Run the cassandra cluster
```sh
docker run -it \
    bitnami/cassandra:latest cqlsh cassandra-server
```

