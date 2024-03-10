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

Compose the services
```sh
docker compose up
```

### Terminal 2
Create the topic
```sh
docker exec kafka kafka-topics.sh --create --topic test-json-topic --bootstrap-server kafka:9092
# Optional: You can see if the topic has been successfully created through the command:
docker exec kafka kafka-topics.sh --list --bootstrap-server kafka:9092
```

Compose the integration-to-kafka pyflink job: kafka_with_json_example_modified.py
```sh
docker exec jobmanager-1 ./bin/flink run -py /opt/flink/usrlib/kafka_with_json_example_modified_no_main.py --jarfile /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar
```

