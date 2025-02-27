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
docker compose up kafka kafka-ui cassandra_stelios cassandra-ui jobmanager taskmanager-1 
# Stop the services
docker compose down kafka kafka-ui cassandra_stelios cassandra-ui jobmanager taskmanager-1 
```

Now you should be able to see 
Now you should be able to see 
- The kafka-ui at localhost:8080
- The cassandra-ui at localhost:8083
- The flink web ui at localhost:8081

### Terminal 4: Download events of the designated gharchive files, thin them and produce them to kafka
```sh
# Create the topic
# Note: Ignore the error on the deletion of the topic as the topic has not been created yet
./delete_and_recreate_topic.sh
# Original producer script. Inside, you can configure the dates between  which to produce the events
# docker compose up python-historical-events-producer

# Producer of 150.000 events of a specific file - created for testing
# purposes
docker compose up python-100.000-events-producer
```

### Attention:
In terminals 5-7, change the pyclientexec option to the host python environment (e.g. /usr/bin/python).

### Terminal 5: Deploy screen 2 pyflink job (job getting the screen 2 data)
```sh
# Attention: The job to accelerate is the following
# Original flink job of screen 2 (execution time: ~170 sec)
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_2_q6_q8_flink_job_one_datastream.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'



# The scripts below are attempts of accelerating the script above.
# They provide another approach for the data ingestion but have failed to be more performant.
# They are added here for completeness

# Testing scripts based on the original job above to accelerate data ingestion performance
# 1. The pyflink job below is the same as the one above but for a single cassandra sink (instead of 6) (execution time: ~70 sec)
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_2_q6_q8_flink_job_one_datastream.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'

# Alternatives 2 and 3 are less efficient and as so can be ignored (job 1 is the most performant)
# 2. Use of functions process() and session.execute_async() (execution time: ~90 sec)
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_2_q6_q8_flink_job_testing.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'

# 3. Use of functions process() and session.execute_concurrent_with_args (execution time: ~95 sec)
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_2_q6_q8_flink_job_testing_concurrent.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'

```

### Terminal 6: Deploy screen 3 pyflink job (job getting the screen 3 data)

```sh
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_3_q9_q10_flink_job.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'
```


### Terminal 7: Deploy screen 4 pyflink job (job getting the screen 4 data)

```sh
docker exec -i jobmanager bash -c './bin/flink run -pyclientexec /usr/bin/python -py /opt/flink/usrlib/screen_4_q11_q15_flink_job.py --config_file_path /opt/flink/usrlib/getting-started-in-docker.ini'  

```





