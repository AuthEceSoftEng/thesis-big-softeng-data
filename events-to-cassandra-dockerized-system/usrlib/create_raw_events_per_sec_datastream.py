'''
Consumes events from kafka topic "raw-events" with schema:
see link: https://docs.github.com/en/rest/using-the-rest-api/github-event-types?apiVersion=2022-11-28
and writes it to kafka topic 'num-of-raw-events-per-sec' with schema:
(timestamp, num_of_raw_events_on_timestamp)
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
env.add_jars("file:///opt/flink/opt/flink-connector-cassandra_2.12-3.2.0-1.18.jar")


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
# Count the number of raw events per second
##################################################################


# Producer to kafka topic "num-of-raw-events-per-sec"
type_info = Types.ROW_NAMED(['timestamp', 'num_of_raw_events_on_timestamp'],\
    [Types.STRING(), Types.INT()])


record_schema = JsonRowSerializationSchema.builder() \
                    .with_type_info(type_info) \
                    .build()

topic_to_produce_into = 'num-of-raw-events-per-sec'

record_serializer = KafkaRecordSerializationSchema.builder() \
    .set_topic(topic_to_produce_into) \
    .set_value_serialization_schema(record_schema) \
    .build()


kafka_producer_num_of_events_per_timestamp = KafkaSink.builder() \
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
                

            
                    
kafka_num_of_raw_events_consumer = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_group_id('raw_events_to_num_of_events_per_sec_consumer_group') \
            .set_topics(topic_to_consume_from) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_props)\
            .set_starting_offsets(KafkaOffsetsInitializer.\
                committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .build()

# Consume original datastream
print(f"start reading data from kafka topic {topic_to_consume_from}")
raw_events_ds = env.from_source( source=kafka_num_of_raw_events_consumer, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="kafka_source")




# Keep only the timestamp of the event as the new datastream and 1 as the number of occurences
# Example: ({"timestamp": "2024-04-01T16:01:18Z", "number_of_events_on_timestamp": "1"})
def keep_timestamp_and_num_of_occurences_only(eventString):
    
    eventDict = eval(eventString)
    event_timestamp = eventDict["created_at"]
    number_of_events_on_timestamp = 1
    
    timestamp_and_num_of_events_on_timestamp = Row(event_timestamp, number_of_events_on_timestamp)
    return timestamp_and_num_of_events_on_timestamp
    
    
    
# Key the datastream elements of the same timestamp to be aggregated
# (use of key_by function))
def key_num_of_events_by_timestamp(timestamp_and_num_of_occurences):
    # Key the row by timestamp (first argument of data element)
    return timestamp_and_num_of_occurences[0]        

# Aggregate the number of events of the same timestamp by reducing the keyed 
# (by timestamp) datastream
def reduce_the_num_of_occurences_by_timestamp(row_keyed_i, row_keyed_j):
    return Row(row_keyed_i[0], row_keyed_i[1] + row_keyed_j[1]) 


# Transform the raw-events datastream into 
# a datastream of counting (event_timestamp, number_of_events_on_timestamp)
num_of_events_per_timestamp_ds = raw_events_ds.map\
                    (keep_timestamp_and_num_of_occurences_only, output_type=type_info)\
                    .key_by(key_num_of_events_by_timestamp, key_type=Types.STRING())\
                    .reduce(reduce_the_num_of_occurences_by_timestamp)
                        
                        
# Produce transformed datastream
num_of_events_per_timestamp_ds.sink_to(kafka_producer_num_of_events_per_timestamp)

# # Print transformed datastream
# num_of_events_per_timestamp_ds.print()




# Execute the flink streaming envoronment
env.execute(os.path.splitext(os.path.basename(__file__))[0])