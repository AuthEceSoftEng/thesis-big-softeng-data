
from pyflink.common import Types, WatermarkStrategy, Row
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, \
    KafkaSink, KafkaRecordSerializationSchema, KafkaOffsetResetStrategy,\
    KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowSerializationSchema

from pyflink.common.completable_future import CompletableFuture
from pyflink.common.job_execution_result import JobExecutionResult
from pyflink.common.job_id import JobID
from pyflink.common.job_status import JobStatus

from pyflink.common.restart_strategy import RestartStrategies


from pyflink.datastream.connectors.kafka import KafkaSource, \
KafkaSink, KafkaRecordSerializationSchema

from argparse import ArgumentParser, RawDescriptionHelpFormatter
from configparser import ConfigParser

import os
from pyflink.datastream.connectors.cassandra import CassandraSink
from cassandra.cluster import Cluster



# Set up the flink execution environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.set_parallelism(1)
env.add_jars("file:///opt/flink/opt/flink-sql-connector-kafka-3.0.2-1.18.jar")
env.add_jars("file:///opt/flink/opt/flink-connector-cassandra_2.12-3.2.0-1.18.jar")

# env.set_restart_strategy(RestartStrategies.\
#     fixed_delay_restart(restart_attempts=3, delay_between_attempts=1000))

env.set_restart_strategy(RestartStrategies.\
    fixed_delay_restart(restart_attempts=1, delay_between_attempts=1000))

env.enable_checkpointing(1000)

# Get the kafka bootstrap servers when deploying the job 
parser = ArgumentParser(prog=f"python {os.path.basename(__file__)}",    
                            description="Warning on function usage: \n\n"
                                "- The pyflink job to start, stop and restore "
                                "should be always the same name as its filename. "
                                "e.g.: The job in the file: my_pyflink_job.py "
                                "must be executed through: "
                                "env.execute('my_pyflink_job')\n\n"
                                "- This is so that the pyflink job id can be retrieved by name"
                                "meaning it must be unique among the jobs deployed "
                                "in the cluster",
                            formatter_class=RawDescriptionHelpFormatter)
parser.add_argument('--config_file_path', required = True)
args = parser.parse_args()
config_parser = ConfigParser()
with open(args.config_file_path, 'r') as config_file:
    config_parser.read_file(config_file)
config = dict(config_parser['default_consumer'])
kafka_bootstrap_servers = config_parser['default_consumer']['bootstrap.servers']






# stats_by_day
stats_type_info = Types.ROW_NAMED(['commits', 'stars', 'forks', \
            'pull_requests', 'day'], [Types.LONG(), Types.LONG(),  \
            Types.LONG(), Types.LONG(), Types.STRING()])

kafka_props = {'enable.auto.commit': 'true',
               'auto.commit.interval.ms': '1000',
               'auto.offset.reset': 'smallest'}
def map_event_string_to_event_dict(event_string):
    return eval(event_string)

# Kafka consumer from kafka topic "near-real-time-raw-events"
topic_to_consume_from = "near-real-time-raw-events"
kafka_consumer_stats = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .set_group_id('raw_events_to_stats_by_day_consumer_group')\
            .set_topics(topic_to_consume_from) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_props)\
            .build()

            
# Consume original datastream
print("Started reading data from kafka topic 'near-real-time-raw-events' to create "
        "topic 'stats_by_day'")
raw_events_ds = env.from_source( source=kafka_consumer_stats, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="kafka_source")\
            .map(map_event_string_to_event_dict)\
                # .set_parallelism(16)




# Filter out events of type that contain no info we need
def filter_no_statistics_events(eventDict):
    event_types_with_info = ["PushEvent", \
    "WatchEvent", "ForkEvent", "PullRequestEvent"]
    event_type = eventDict["type"]
    
    # Acceleration attempt:
    if event_type == "PushEvent":
        # Keep PushEvents if number_of_conmmits <= 200 or if size == distinct_size
        number_of_commits = eventDict["payload"]["distinct_size"]
        if (number_of_commits <= 100) or (number_of_commits <= 200
        and eventDict["payload"]["size"] == eventDict["payload"]["distinct_size"]):
            return True
        else:
            return False
    elif event_type == "WatchEvent":
        return True
    elif event_type == "ForkEvent":
        return True
    elif event_type == "PullRequestEvent":
        # Filter out PullRequestEvents that do not open or close issues
        if eventDict["payload"]["action"] == "opened" or \
            eventDict["payload"]["action"] == "reopened":
            return True
    # If none of the above works, return False (filter the event out)
    return False

    
# Function for the original raw-events datastream
def extract_statistics_and_create_row(eventDict):
    # eventDict = json.loads(eventString)
    event_type = eventDict["type"]
    
    
    # Removing trailing characters after T that gives us the time
    # leaves us with the day 
    # Split 2024-01-01T00:00:01Z into 2024-01-01 and T00:00:01Z 
    # and keep only the first one
    day = eventDict["created_at"].split('T', 1)[0]
    # Initialize the statistics per day
    commits = 0
    # open_issues = 0
    # closed_issues = 0
    stars = 0
    forks = 0
    pull_requests = 0
                            
    if event_type == "PushEvent":
        commits = eventDict["payload"]["distinct_size"]
    elif event_type == "WatchEvent":
        stars = 1
    elif event_type == "ForkEvent":
        forks = 1
    elif event_type == "PullRequestEvent" and \
        (eventDict["payload"]["action"] == "opened" or \
            eventDict["payload"]["action"] == "reopened"):
        pull_requests = 1
    else: 
        raise ValueError(f"Event type {event_type} is not filtered")
         
    return Row(commits, stars, forks, pull_requests, day)
    

max_concurrent_requests = 1000
cassandra_host = 'cassandra_stelios'
cassandra_port = 9142
cassandra_keyspace = "near_real_time_data"
stats_table = 'stats_by_day'
print(f"Inserting data from kafka topics into Cassandra tables:\n"
        "T1: stats_by_day, T2_3: most_popular_repos_by_day,\n"
        "T4: most_popular_languages_by_day, T5: most_popular_languages_by_day")

stats_ds = raw_events_ds.filter(filter_no_statistics_events)\
                        .map(extract_statistics_and_create_row, \
                           output_type=stats_type_info)\


upsert_element_into_stats_by_day_q1 = \
            f"UPDATE {cassandra_keyspace}.{stats_table} \
                    SET commits = commits + ?, stars = stars + ?, \
                    forks = forks + ?, pull_requests = pull_requests + ? WHERE \
                    day = ?;"
                    
cassandra_sink_q1 = CassandraSink.add_sink(stats_ds)\
    .set_query(upsert_element_into_stats_by_day_q1)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()







# # Print transformed datastream
# stats_ds.print()

# endregion



if __name__ =='__main__':
    
    # Create cassandra keyspace if not exist
    cluster = Cluster([cassandra_host],port=cassandra_port, connect_timeout=10)
    session = cluster.connect()
    cassandra_keyspace = "near_real_time_data"
    create_keyspace = f"CREATE KEYSPACE IF NOT EXISTS \
        {cassandra_keyspace} WITH replication = \
        {{'class': 'SimpleStrategy', 'replication_factor': '1'}}\
        AND durable_writes = true;"
    session.execute(create_keyspace)
    session = cluster.connect(cassandra_keyspace, wait_for_all_pools=True)
    session.execute(f'USE {cassandra_keyspace}')

    
    # Screen 1
    stats_table = 'stats_by_day'
    create_stats_table =  \
                f"CREATE TABLE IF NOT EXISTS {cassandra_keyspace}.{stats_table} \
                (day text, commits counter, stars counter, pull_requests counter, \
                forks counter, PRIMARY KEY (day));"
    session.execute(create_stats_table)




    cluster.shutdown()
    
    # Execute the flink streaming environment
    env.execute(os.path.splitext(os.path.basename(__file__))[0])







# Using as guide the flink jobs (e.g usrlib/screen_2_q6_q8_flink_job_q6b_q7h.py)

# In main: Create table stats (use a new keyspace)
# Consume from kafka datastream, store into Cassandra

# Docker exec into cassandra to check if the data is inserted correctly
# Use the docker service test_near_real_time_queries to get the ingested data in descending form (as shown in the login screen)

# To run add this file in the volumes of the ;;;'taskmanager-near-real-time' and 'jobmanager' docker services. 


# Repeat for tables language, topics, most starred 