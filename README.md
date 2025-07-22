# Design and implementation of streaming and static data analysis platform for software process data
Diploma thesis for processing Big Data from software processes. Incorporates Apache Kafka, Flink and Cassandra to handle real time and historical GitHub events for software processes analyses.

The running app consists of the real time data analysis and the historical data analysis and runs on a Linux terminal.


## Contents
[Ingest real time GitHub events](#Ingest-real-time-events)
- [Step 1: Pull and build the docker images](#step-1-pull-and-build-the-docker-images)
- [Step 2: Compose Kafka, Flink and Cassandra](#step-2-compose-kafka-flink-and-cassandra)
- [Step 3: Compose the real time GitHub events Kafka Producer](#step-3-compose-the-real-time-gitHub-events-kafka-producer)
- [Step 4: Ingest real GitHub events using a Pyflink job](#step-4-ingest-real-github-events-using-a-pyflink-job)
- [Step 5: Expose GitHub events to the UI and deploy it](#step-5-expose-github-events-to-the-ui-and-deploy-it)


<!-- 2. [Ingest historical GitHub events](#Ingest-historical-events) -->


## Ingest real time events 

### Step 1: Pull and build docker images 

```sh
# For services: 
# kafka
docker image pull bitnami/kafka:3.9

# kafka-ui
docker pull provectuslabs/kafka-ui:v0.7.2

# cassandra
docker image pull cassandra:4.1.7

# cassandra-ui
docker pull ipushc/cassandra-web:v1.1.5

# jobmanager, taskmanager
docker build -f Dockerfile-pyflink -t pyflink:1.18.1 .

# python-real-time-events-producer, python-data-exposing-server, 
# python-flask-app, python-historical-events-producer 
docker build -f Dockerfile-python -t python:3.10-script-executing-image . 

```


### Step 2: Compose Kafka, Flink and Cassandra
```sh
# Run bash script to create directories for the kafka docker container
./helpers/setup-kafka-and-ui.sh

# Compose the actual services
docker compose up -d kafka kafka-ui jobmanager taskmanager-real-time cassandra_host cassandra-ui 
```

### Step 3: Compose the real time GitHub events Kafka Producer
```sh
docker compose up -d python-real-time-events-producer
```

Pyflink job to store the data of screen 1 in the UI

Deploy the real time job for screen 1:
### Step 4: Ingest real GitHub events using a Pyflink job
```sh
docker exec -d -i  jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_1_q1_q5_flink_job.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'  
```

### Step 5: Expose GitHub events to the UI and deploy it
```sh
docker compose up -d event-data-exposing-server events-flask-app
```




## Ingest historical events 
All terminals below are in the project's root directory

### Step 1: Run bash script to create directories for the kafka docker container

Execute the bash script below if it was not executed already for the real time GitHub events ingestion part
```sh
./helpers/setup-kafka-and-ui.sh
```

### Step 2: Start services kafka, cassandra and flask app ui
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
- The flink web ui at localhost:8081


### Step 3: Download events of the designated gharchive files, thin them and produce them to kafka
```sh

# For the historical analysis, choose the events of December you want to download and thin in files historical-files-thinner, historical-files-thinner-2 (and sililarly for 3 and 4) in lines:
# starting_date_formatted = '2024-12-04-0'
# ending_date_formatted = '2024-12-04-4' 
# (Change the dates as you choose)
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
In terminals 5-7, change the pyclientexec option to your host's python path (e.g. /usr/bin/python).

### Step 4: Deploy screen 2 pyflink jobs (job getting the screen 2 data)
```sh
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_2_q6_q8_flink_job_q6b_q7h.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_2_q6_q8_flink_job_q8b_q8h.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'
```

### Step 5: Deploy screen 3 pyflink job (job getting the screen 3 data)

```sh
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_3_q9_q10_flink_job.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'
```


### Step 6: Deploy screen 4 pyflink job (job getting the screen 4 data)

```sh
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_4_q11_q15_flink_job.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'  
```

### Step 7 (optional): Cancel all jobs (you can also do so manually from the UI)
```sh
docker compose up cancel-all-flink-jobs
```

### Step 8 (optional): Delete messages of the 'historical-raw-events' topic if the topic takes up too much space
```sh
# Free up the space of the topic (delete its messages and make its size = 0)
cd usrlib
./delete_and_recreate_topic.sh
```


