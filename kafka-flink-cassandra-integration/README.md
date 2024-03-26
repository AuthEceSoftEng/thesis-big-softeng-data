## Kafka, Flink and Cassandra: Integration guide

The guide below describes how to run Kafka, Flink and Cassandra on docker. 
The different terminals numbered below are used to keep the deployment of the docker services modular.


### Terminal 1 - Image builds and docker compose
Execute the bash script below to setup for the compose of the kafka and kafka-ui services
```sh
sudo ./helpers/setup-kafka-and-ui.sh
```
Build the images below: 
```sh
# Image to run the producer and consumer python scripts
docker build -f Dockerfile-python --tag python:3.10-usrlib-as-workdir .
#  Image to run the pyflink job 
docker build -f Dockerfile-pyflink --tag pyflink:latest .
```


Run the Kafka, Flink and Cassandra services.
```sh
docker compose up kafka kafka-ui taskmanager jobmanager cassandra
```
You should be able to see the Kafka UI in localhost:8080 and the Flink job execution UI in localhost:8081

### Terminal 2: Create the kafka topics and produce messages
Create the topics raw-events with the raw event data and event-count with the count of events per type (PushEvent, WatchEvent etc).
```sh
docker exec kafka kafka-topics.sh --create --topic raw-events --bootstrap-server kafka:9092
docker exec kafka kafka-topics.sh --create --topic event-count --bootstrap-server kafka:9092
# Optional: You can see if the topis have been successfully created through the command:
docker exec kafka kafka-topics.sh --list --bootstrap-server kafka:9092
# Optional: Delete the topic data if needed:
docker exec kafka kafka-topics.sh --delete <topic-name> --bootstrap-server kafka:9092
```


Start the "python-producer" service. It creates the kafka topic "raw-events" and starts producing messages into it.


```sh
docker compose up python-producer 
```
Now, you should be able to see the raw events as messages in the topic raw-events in the Kafka UI at localhost:8080.<br>
To stop the "python-producer", press Ctrl + C in the terminal.


### Terminal 3 - Consume messages from Cassandra
Access the cassandra container through cqlsh and create a keyspace
 
```sh
docker exec -it cassandra cqlsh
# Once the cassandra cli starts:
cqlsh> CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy'};
```
If the "cassandra" service has not started you will get the error: 
```sh
"Connection error: ('Unable to connect to any servers', {'127.0.0.1:9042': ConnectionRefusedError(111, "Tried connecting to [('127.0.0.1', 9042)]. Last error: Connection refused")})"
```

### Terminal 4
Compose the "python-consumer" service. It should consume messages from the topic "raw-events" and send it to cassandra.<br>
```sh
docker compose up python-consumer 
```
To stop the "python-consumer" service, press Ctrl + C.



### Terminal 3 (revisited) - Optional: Go back to terminal 3 and make queries to the Cassandra database through cqlsh
You can run simple cql commands on the cqlsh terminal. Some simple examples are shown below:
```sh
# To see the table mykeyspace.events structure:
cqlsh> DESC mykeyspace.events;
# Example 1: Count the events in the database
cqlsh> SELECT COUNT(*) FROM mykeyspace.events;
# Example 2: Retrieve the id and actor information of 5 events maximum of type CreateEvent:
cqlsh> SELECT id, actor FROM mykeyspace.events WHERE type = 'CreateEvent' LIMIT 5 ALLOW FILTERING;

```

You can also access the topic "raw-events"' data through the Kafka UI at localhost:8080.




### Terminal 5 - Run the pyflink job to calculate the new events in real time 
Compose the pyflink job: num-of-events-per-type.py
```sh
docker exec jobmanager-1 ./bin/flink run -py /opt/flink/usrlib/num-of-events-per-type.py --jarfile /opt/flink/connectors/flink-sql-connector-kafka-3.0.2-1.18.jar
```

After running the command above you should be able to see the following:
- The python integration script "num-of-events-per-type.py" as a Flink job committed for execution in the Flink UI at localhost:8081.
- The number of occurences of events per type in the topic event-count calculated from the initial raw-events stream at localhost:8080.<br>

To terminate the pyflink job, you can select "Cancel job" in the Flink UI at localhost:8081.