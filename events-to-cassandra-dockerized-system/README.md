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
# or
docker build -f Dockerfile-python -t python:3.10-script-executing-image-with-requests_sse . 
```


### Terminal 2: Run bash script to create directories for the kafka docker container
```sh
./helpers/setup-kafka-and-ui.sh
```

### Terminal 3: Start services kafka, cassandra and flask app ui
```sh
# Start the services
docker compose up kafka kafka-ui jobmanager taskmanager-1 cassandra_host cassandra-ui python-flask-app
# Stop the services
docker compose down kafka kafka-ui jobmanager taskmanager-1 cassandra_host cassandra-ui python-flask-app
```

Now you should be able to see 
- The kafka-ui at localhost:8080
- The cassandra-ui at localhost:8083
- The flask app UI at localhost:5000
- (Optionally) All database data exposed (if any): the addresses exposed are in the file: 'events-to-cassandra-dockerized-system/flask-material-dashboard-with-counters/server.py'
- The flink web ui at localhost:8081


### Terminal 4: Download events of the designated gharchive files, thin them and produce them to kafka
```sh
docker compose up python-historical-events-thinner # (for a single downloaded and thinner)
# For multiple downloaders and thinners running in parallel:
docker compose up python-historical-events-thinner-2
docker compose up python-historical-events-thinner-3
docker compose up python-historical-events-thinner-4

# Create the topic
# Note: Ignore the error on the deletion of the topic as the topic has not been created yet
./delete_and_recreate_topic.sh
docker compose up python-historical-events-producer
```


### Attention:
In terminals 5-7, change the pyclientexec option to the host python environment (e.g. /usr/bin/python).

### Terminal 5: Deploy screen 2 pyflink job (job getting the screen 2 data)
```sh
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_2_q6_q8_flink_job.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'

# Screen 2 job split in 2 
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_2_q6_q8_flink_job_q6b_q7h.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_2_q6_q8_flink_job_q8b_q8h.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'


# Legacy screen 2 parts
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_2_q6_q8_flink_job_q6b_q7b_backup_27_4.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_2_q6_q8_flink_job_q7h_q8h_backup_27_4.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'


```

### Terminal 6: Deploy screen 3 pyflink job (job getting the screen 3 data)

```sh
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_3_q9_q10_flink_job.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'

# Legacy
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_3_q9_q10_flink_job_backup_27_4.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'
```


### Terminal 7: Deploy screen 4 pyflink job (job getting the screen 4 data)

```sh
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_4_q11_q15_flink_job.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'  

# Legacy
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_4_q11_q15_flink_job_backup_27_4.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'  

```

### Terminal 8: Cancel all jobs (you can also do so manually from the UI)
```sh
docker compose up cancel-all-flink-jobs
```

### Terminal 9 (optional): Delete messages of the 'historical-raw-events' topic if the topic takes up too much space
```sh
# Free up the space of the topic (delete its messages and make its size = 0)
cd usrlib
./delete_and_recreate_topic.sh
```





## Ingest real time events 

### Terminal 1: Compose kafka, cassandra, flink, expose server data, run the flask app
```sh
docker compose up (-d) kafka kafka-ui cassandra_host cassandra-ui jobmanager taskmanager-real-time 
```

### Terminal 2: Producer
```sh
docker compose up (-d) python-real-time-events-producer
```

Pyflink job to store the data of screen 1 in the UI

Deploy the near real time job for screen 1:
### Terminal 3: Pyflink job 1: Stats and popularity insights 
```sh
docker exec (-d) -i  jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_1_q1_q5_flink_job.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'  
```

### Terminal 4: Expose data to ingest in the UI
```sh
docker compose up (-d) event-data-exposing-server 
```

### Terminal 5: Deploy the UI
```sh
docker compose up (-d) events-flask-app
```










