## Kafka and Cassandra: Integration guide
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

### Deploy the Kafka broker, Kafka UI and Cassandra database in the docker container
```sh
docker compose up
```
