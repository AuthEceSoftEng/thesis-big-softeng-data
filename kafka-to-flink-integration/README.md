# Integrate Kafka and Flink

The guide below describes how to run an integration script of Kafka and Flink on docker. The different terminals numbered below can be used for a more modular deployment of the docker services.

### Terminal 1
Build the pyflink image to run the flink job
```sh
docker build -f Dockerfile --tag pyflink:latest .
```

Execute the bash script below to setup for the compose of the kafka and kafka-ui services
```sh
sudo ./helpers/setup-kafka-and-ui.sh
```

Compose the services below:
```sh
docker compose up kafka kafka-ui taskmanager jobmanager
```
You should be able to see the Kafka UI in localhost:8080 and the Flink job execution UI in localhost:8081

### Terminal 2
Compose the producer script to write raw event data from a file to the topic
```sh
docker compose up python-producer
```
You should be able to see the raw events as messages in the topic raw-events in the Kafka UI at localhost:8080

### Terminal 3
Create the topics raw-events with the raw event data and event-count with the count of events per type (PushEvent, WatchEvent etc).
```sh
docker exec kafka kafka-topics.sh --create --topic raw-events --bootstrap-server kafka:9092
docker exec kafka kafka-topics.sh --create --topic event-count --bootstrap-server kafka:9092
# Optional: You can see if the topis have been successfully created through the command:
docker exec kafka kafka-topics.sh --list --bootstrap-server kafka:9092
```

Compose the pyflink job: num-of-events-per-type.py
```sh
docker exec jobmanager-1 ./bin/flink run -py /opt/flink/usrlib/num-of-events-per-type.py --jarfile /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar
```

After running the command above you should be able to see the following:
- The python integration script "num-of-events-per-type.py" as a Flink job committed for execution in the Flink UI at localhost:8081.
- The number of occurences of events per type in the topic event-count calculated from the initial raw-events stream at localhost:8080.
