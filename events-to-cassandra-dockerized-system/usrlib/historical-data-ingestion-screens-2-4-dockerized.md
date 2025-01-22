# Kafka-Flink-Cassandra simple integration guide  

### Terminal 1: Pull and build docker images 

```sh
cd events-to-cassandra-dockerized-system
# kafka
docker image pull bitnami/kafka
# kafka-ui
docker image pull provectuslabs/kafka-ui
# cassandra
docker image pull cassandra:4.1.7
# cassandra-ui
docker image pull ipushc/cassandra-web

# # Prerequisite: python should be installed 
# # To check if python3 is installed:
# python3 --version

# jobmanager, taskmanager
docker build -f Dockerfile-pyflink -t pyflink:1.18.1 .
#  python-historical-events-producer python-data-exposing-server, python-flask-app
docker build -f Dockerfile-python -t python:3.10-script-executing-image . 
```


### Terminal 2: Run command to execute kafka container
```sh
# Base dir: /events-to-cassandra-dockerized-system
cd /events-to-cassandra-dockerized-system
./helpers/setup-kafka-and-ui.sh
```

### Terminal 3: Start services kafka, flink cluster, cassandra and flask app ui
```sh
# Start the services
docker compose up kafka kafka-ui jobmanager taskmanager cassandra cassandra-ui python-flask-app
# Stop the services
docker compose down kafka kafka-ui jobmanager taskmanager cassandra cassandra-ui python-flask-app
```

Now you should be able to see 
- The kafka-ui at localhost:8080
- The flink web ui at localhost:8081
- The cassandra-ui at localhost:8083
- The flask app UI at localhost:5000
- (Optionally) All data exposed: the addresses exposed are in the file: 'events-to-cassandra-dockerized-system/flask-material-dashboard-with-counters/server.py'


### Terminal 4: Create keyspace 'prod_gharchive' if not exists 
```sh
# Check if the cassandra container is running (status should be 'Up <running_time>'):
docker ps --format "table {{.Names}} \t {{.Status}}"
# Create keyspace 'prod_gharchive' if not exists 
docker exec -it cassandra /bin/bash
cqlsh cassandra
CREATE KEYSPACE IF NOT EXISTS prod_gharchive WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '1'} AND durable_writes = true;
# #(Optional) Check if the keyspace was created
# describe keyspace prod_gharchive;

# Run the python-data-exposing-server service
docker compose up python-data-exposing-server 
```


### Terminal 5: Download events of the designated gharchive files, thin them and produce them to kafka
```sh
docker compose up python-historical-events-producer
```


### Attention:
In terminals 6-8, change the pyclientexec option to the host python environment

### Terminal 6: Deploy screen 2 pyflink job (job getting the screen 2 data)
```sh
# Deploy the pyflink job 
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec {/usr/bin/python} --jarfile /opt/flink/opt/flink-sql-connector-kafka-3.0.2-1.18.jar -py /opt/flink/usrlib/screen_2_q6_q8_flink_job.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'

# Without the jar
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec {/usr/bin/python} -py /opt/flink/usrlib/screen_2_q6_q8_flink_job.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'

```


### Terminal 7: Deploy screen 3 pyflink job (job getting the screen 3 data)

```sh
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec {/usr/bin/python} --jarfile /opt/flink/opt/flink-sql-connector-kafka-3.0.2-1.18.jar -py /opt/flink/usrlib/screen_3_q9_q10_flink_job.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'
```


### Terminal 8: Deploy screen 4 pyflink job (job getting the screen 4 data)

```sh
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec {/usr/bin/python} --jarfile /opt/flink/opt/flink-sql-connector-kafka-3.0.2-1.18.jar -py /opt/flink/usrlib/screen_4_q11_q15_flink_job.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'  
```


### Terminal 9 (optional): Delete messages of the 'historical-raw-events' topic if the topic takes up too much space
```sh
# Free up the space of the topic (delete its messages and make its size = 0)
cd usrlib
python delete_topic_messages.py getting-started-in-docker.ini --wait_for_topic_size_to_reduce_to_0 True
```








