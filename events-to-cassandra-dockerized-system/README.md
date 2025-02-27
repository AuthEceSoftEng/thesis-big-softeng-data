# Integrate Kafka, Flink and Cassandra in Docker   


## Ingest historical events 
All terminals below are in the events-to-cassandra-dockerized-system directory

### Terminal 1: Pull and build docker images 

```sh
# For services: 
# kafka
docker image pull bitnami/kafka:3.9

# kafka-ui
docker image pull provectuslabs/kafka-ui:latest

# cassandra
docker image pull cassandra:4.1.7

# cassandra-ui
docker image pull ipushc/cassandra-web:latest

# jobmanager, taskmanager
docker build -f Dockerfile-pyflink -t pyflink:1.18.1 .

# python-historical-events-producer python-data-exposing-server, python-flask-app
docker build -f Dockerfile-python -t python:3.10-script-executing-image . 
```


### Terminal 2: Run bash script to create directories for the kafka docker container
```sh
./helpers/setup-kafka-and-ui.sh
```

### Terminal 3: Start services kafka, cassandra and flask app ui
```sh
# Start the services
docker compose up kafka kafka-ui cassandra_stelios cassandra-ui python-flask-app
# Stop the services
docker compose down kafka kafka-ui cassandra_stelios cassandra-ui python-flask-app
```

Now you should be able to see 
- The kafka-ui at localhost:8080
- The cassandra-ui at localhost:8083
- The flask app UI at localhost:5000
- (Optionally) All database data exposed (if any): the addresses exposed are in the file: 'events-to-cassandra-dockerized-system/flask-material-dashboard-with-counters/server.py'

### Terminal 4: Start flink cluster services
```sh
# Start the services
docker compose up -d jobmanager && docker compose up taskmanager-1 
# Stop the services
docker compose down jobmanager taskmanager-1
```

Now you should be able to see 
- The flink web ui at localhost:8081


### Terminal 5: Download events of the designated gharchive files, thin them and produce them to kafka
```sh
# Create the topic
# Note: Ignore the error on the deletion of the topic as the topic has not been created yet
./delete_and_recreate_topic.sh
docker compose up python-historical-events-producer
```


### Attention:
In terminals 6-8, change the pyclientexec option to the host python environment (e.g. /usr/bin/python).

### Terminal 6: Deploy screen 2 pyflink job (job getting the screen 2 data)
```sh
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_2_q6_q8_flink_job.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'

# Alternatives to accelerate data ingestion performance
# Use of functions process() and session.execute_async()
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_2_q6_q8_flink_job_testing.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'

# Use of functions process() and session.execute_concurrent_with_args
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_2_q6_q8_flink_job_testing_concurrent.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'

```

### Terminal 7: Deploy screen 3 pyflink job (job getting the screen 3 data)

```sh
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_3_q9_q10_flink_job.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'
```


### Terminal 8: Deploy screen 4 pyflink job (job getting the screen 4 data)

```sh
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_4_q11_q15_flink_job.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'  

```


### Terminal 9 (optional): Delete messages of the 'historical-raw-events' topic if the topic takes up too much space
```sh
# Free up the space of the topic (delete its messages and make its size = 0)
cd usrlib
./delete_and_recreate_topic.sh
```






