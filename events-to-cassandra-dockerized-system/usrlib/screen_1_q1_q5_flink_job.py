

# Using as guide the flink jobs (e.g usrlib/screen_2_q6_q8_flink_job_q6b_q7h.py)

# In main: Create table stats (use a new keyspace)
# Consume from kafka datastream, store into Cassandra

# Docker exec into cassandra to check if the data is inserted correctly
# Use the docker service test_near_real_time_queries to get the ingested data in descending form (as shown in the login screen)

# To run add this file in the volumes of the ;;;'taskmanager-near-real-time' and 'jobmanager' docker services. 


# Repeat for tables language, topics, most starred 