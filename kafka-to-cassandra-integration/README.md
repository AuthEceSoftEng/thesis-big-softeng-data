## Kafka and Cassandra: Integration guide

Move to directory "kafka-to-cassandra-integration"
```sh
cd kafka-to-cassandra-integration
```
Build the docker image to run the producer.py script
```sh
docker build -f Dockerfile-python --tag python:3.10-script-executing-image .
```
Execute the bash script below to setup for the compose of the kafka and kafka-ui services
```sh
sudo ./helpers/setup-kafka-and-ui.sh
```

Run the kafka, kafka-ui and cassandra services
```sh
docker compose up kafka kafka-ui cassandra
```
In another terminal, compose the producer-python service. It creates the kafka topic "events-topic" and starts producing messages into it.<br>
To stop the "python-producer" service, press Ctrl + C.
```sh
docker compose up python-producer 
```
Access the cassandra container through cqlsh. Wait a moment for the cassandra cluster to run or else you will get the error: 
```sh
"Connection error: ('Unable to connect to any servers', {'127.0.0.1:9042': ConnectionRefusedError(111, "Tried connecting to [('127.0.0.1', 9042)]. Last error: Connection refused")})"
```
 
```sh
docker exec -it cassandra cqlsh
# Once the cassandra cli starts:
cqlsh> CREATE KEYSPACE IF NOT EXISTS mykeyspace
```

In another terminal, compose the python-consumer service. It should consume messages from the topic events-topic and send it to cassandra.<br>
To stop the "python-consumer" service, press Ctrl + C.
```sh
docker compose up python-consumer 
```
