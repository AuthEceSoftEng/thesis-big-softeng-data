## Kafka and Cassandra: Integration guide

The guide below describes how to run Kafka and Cassandra on docker. The different terminals are


### Terminal 1
Execute the bash script below to setup for the compose of the kafka and kafka-ui services
```sh
sudo ./helpers/setup-kafka-and-ui.sh
```

Run the "kafka", "kafka-ui" and "cassandra" services. After the services begin to run, you should be able to see the kafka-ui service running in localhost:8080. 
```sh
docker compose up kafka kafka-ui cassandra
```
### Terminal 2
Build the docker image to run the "python-producer" service
```sh
docker build -f Dockerfile-python --tag python:3.10-script-executing-image .
```
Start the "python-producer" service. It creates the kafka topic "events-topic" and starts producing messages into it. After the topic has been created, you should be able to see it in the "kafka-ui" in localhost:8080. <br>
To stop the "python-producer", press Ctrl + C.

```sh
docker compose up python-producer 
```

To delete the events-topic data, stop the producer service and execute the bash script below:
```sh
./helpers/delete-kafka-topic.sh
```

### Terminal 3
Compose the "python-consumer" service. It should consume messages from the topic "events-topic" and send it to cassandra.<br>
To stop the "python-consumer" service, press Ctrl + C.
```sh
docker compose up python-consumer 
```

### Terminal 4
Access the cassandra container through cqlsh. 
 
```sh
docker exec -it cassandra cqlsh
# Once the cassandra cli starts:
cqlsh> CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy'};
```

If the "cassandra" service has not started you will get the error: 
```sh
"Connection error: ('Unable to connect to any servers', {'127.0.0.1:9042': ConnectionRefusedError(111, "Tried connecting to [('127.0.0.1', 9042)]. Last error: Connection refused")})"
```

Now you can run simple cql commands on the cqlsh terminal. Some simple examples are shown below:
```sh
# To see the table mykeyspace.events structure:
cqlsh> DESC mykeyspace.events;
# Example 1: Count the events in the database
cqlsh> SELECT COUNT(*) FROM mykeyspace.events;
# Example 2: Retrieve the id and actor information of 5 events maximum of type CreateEvent:
cqlsh> SELECT id, actor FROM mykeyspace.events WHERE type = 'CreateEvent' LIMIT 5;

```

You can also access the topic data through the "kafka-ui" at localhost:8080.
