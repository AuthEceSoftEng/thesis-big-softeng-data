## Kafka and Cassandra: Integration guide

```sh
python --version # Should be 3.10.12
python -m pip install -r requirements.txt

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

Deploy the Kafka broker, Kafka UI and Cassandra database in the docker container
```sh
docker compose up
```


Terminal 1: Create the kafka topic and run the producer.py
```sh
docker exec -it kafka bash
kafka-topics.sh --create --topic events-topic --bootstrap-server kafka:9092
python producer.py
```


Terminal 2: Run the cassandra cluster
```sh
docker run -it \
    bitnami/cassandra:latest cqlsh cassandra-server
```

