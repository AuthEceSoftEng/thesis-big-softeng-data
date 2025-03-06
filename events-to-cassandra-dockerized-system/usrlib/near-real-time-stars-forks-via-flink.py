'''
Consumes events from kafka topic "raw-events" with schema:
see link: https://docs.github.com/en/rest/using-the-rest-api/github-event-types?apiVersion=2022-11-28
and writes it to another kafka topic ("repos") with schema:
(full_name, language, topics)
'''

# Template: 
# /home/xeon/Thesis/local-kafka-flink-cassandra-integration/presentation-8-demo/count-repos-with-kafka.py

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


# Set up the flink execution environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.set_parallelism(1)

env.add_jars("file:///opt/flink/opt/flink-sql-connector-kafka-3.0.2-1.18.jar")


env.set_restart_strategy(RestartStrategies.\
    fixed_delay_restart(restart_attempts=3, delay_between_attempts=1000))

# Enable checkpointing
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

# Parse the configuration to setup the consumer
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
config_parser = ConfigParser()
with open(args.config_file_path, 'r') as config_file:
    config_parser.read_file(config_file)

config = dict(config_parser['default_consumer'])

kafka_bootstrap_servers = config_parser['default_consumer']['bootstrap.servers']



##################################################################
# Forks and stars
##################################################################


# Event types for forks and stars of repos
fork_and_watch_events = ["ForkEvent", \
    "WatchEvent"]


# Producer to kafka topic "real-time-stars-forks"
type_info = Types.ROW_NAMED(['username', 'event_type', 'repo_name', 'timestamp'],\
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()])


record_schema = JsonRowSerializationSchema.builder() \
                    .with_type_info(type_info) \
                    .build()

topic_to_produce_into = 'near-real-time-stars-forks'

record_serializer = KafkaRecordSerializationSchema.builder() \
    .set_topic(topic_to_produce_into) \
    .set_value_serialization_schema(record_schema) \
    .build()

# kafka_bootstrap_servers = "localhost:45945"

kafka_producer_stars_forks = KafkaSink.builder() \
    .set_bootstrap_servers(kafka_bootstrap_servers) \
    .set_record_serializer(record_serializer) \
    .build()

                  

    
# Kafka consumer from kafka topic "near-real-time-raw-events"
topic_to_consume_from = "near-real-time-raw-events"
kafka_props = {'enable.auto.commit': 'true',
               'auto.commit.interval.ms': '1000',
               'auto.offset.reset': 'smallest'}

# .set_starting_offsets(KafkaOffsetsInitializer.\
#                 committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
                

            
                    
kafka_consumer_stars_forks = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_group_id('near_real_time_events_to_stars_forks_consumer_group') \
            .set_topics(topic_to_consume_from) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_props)\
            .set_starting_offsets(KafkaOffsetsInitializer.\
                committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .build()

# Consume original datastream
print(f"start reading data from kafka topic {topic_to_consume_from}")
raw_events_ds = env.from_source( source=kafka_consumer_stars_forks, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="kafka_source")


# Filter out events not of type WatchEvent or ForkEvent
def filter_no_info_events(eventString):
    global event_types_with_info 
    eventDict = eval(eventString)
    event_type = eventDict["type"]
    
    # # Acceleration attempt:
    if event_type not in fork_and_watch_events:
        return False
    else:
        return True
    
    
# Function for the original raw-events datastream
def extract_fork_and_star_event_info(eventString):
        eventDict = eval(eventString)
        
        username = eventDict["actor"]
        repo_name = eventDict["repo"]["full_name"]
        event_type = eventDict["type"]
        timestamp = eventDict["created_at"]
        
        repo_row = Row(username, event_type, repo_name, timestamp)
        return repo_row

# Transform the near-real-time-stars-forks datastream into 
# a datastream of watch and fork events:
near_real_time_stars_forks_ds = raw_events_ds.filter(filter_no_info_events)\
                        .map(extract_fork_and_star_event_info, \
                           output_type=type_info)

# Produce transformed datastream
near_real_time_stars_forks_ds.sink_to(kafka_producer_stars_forks)

# # Print transformed datastream
# near_real_time_stars_forks_ds.print()




# Execute the flink streaming environment
env.execute(os.path.splitext(os.path.basename(__file__))[0])