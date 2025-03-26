'''

Template: /home/xeon/Thesis/local-kafka-flink-cassandra-integration/presentation-10-demo/task-2-store-tables-repos-and-stats/near-real-time-bots-vs-humans-via-flink.py
Template (also): 
/home/xeon/Thesis/local-kafka-flink-cassandra-integration/presentation-8-demo/count-repos-with-kafka.py

To parse the kafka configuration see:
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
'''


from pyflink.common import Types, WatermarkStrategy, Row
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.restart_strategy import RestartStrategies

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.functions import RuntimeContext, KeyedProcessFunction, \
    RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.formats.json import JsonRowSerializationSchema

from pyflink.datastream.connectors.kafka import KafkaSource, \
    KafkaOffsetResetStrategy, KafkaOffsetsInitializer
from pyflink.datastream.connectors.cassandra import CassandraSink


from argparse import ArgumentParser, RawDescriptionHelpFormatter
from configparser import ConfigParser

from cassandra.cluster import Cluster 
from datetime import datetime
import os

from pyflink.datastream.window import CountTumblingWindowAssigner
from pyflink.datastream.functions import AllWindowFunction, ProcessAllWindowFunction, ProcessFunction
from typing import Iterable, List
from pyflink.datastream.window import CountWindow, GlobalWindow


# Note: Sections I-IV are used by all the transformed datastreams-to-table processes

# I. Set up the flink execution environment
# region 
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
# env.set_parallelism(2)

# Connectors in /opt/flink/opt
env.add_jars("file:///opt/flink/opt/flink-sql-connector-kafka-3.0.2-1.18.jar")
env.add_jars("file:///opt/flink/opt/flink-connector-cassandra_2.12-3.2.0-1.18.jar")
# env.add_jars("file:///opt/flink/opt/flink-streaming-scala_2.12-1.18.1.jar")
# env.add_jars("file:///opt/flink/opt/flink-netty-tcnative-dynamic-2.0.62.Final-18.0.jar")


env.set_restart_strategy(RestartStrategies.\
    fixed_delay_restart(restart_attempts=3, delay_between_attempts=1000))

#endregion 

# II. Configure connection of flink to kafka 
# region
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

# endregion

# III. Create a Cassandra cluster, connect to it and use a keyspace
# region

cassandra_host = 'cassandra'
cassandra_port = 9142
cluster = Cluster([cassandra_host],port=cassandra_port, connect_timeout=10)

# Connect without creating keyspace. Once connected create the keyspace
session = cluster.connect()
create_keyspace = "CREATE KEYSPACE IF NOT EXISTS "\
    "prod_gharchive WITH replication = {'class': 'SimpleStrategy', "\
    "'replication_factor': '1'} AND durable_writes = true;"
session.execute(create_keyspace)

cassandra_keyspace = 'prod_gharchive'
session = cluster.connect(cassandra_keyspace, wait_for_all_pools=True)
session.execute(f'USE {cassandra_keyspace}')
# endregion


# IV. Consume the original datastream 'historical-raw-events'
#region 

kafka_props = {'enable.auto.commit': 'true',
               'auto.commit.interval.ms': '1000',
               'auto.offset.reset': 'smallest'}


topic_to_consume_from = "historical-raw-events"


second_screen_consumer_group_id_1 = 'second_screen_consumer_group_id_1'


kafka_consumer_second_screen_source_1 = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_starting_offsets(KafkaOffsetsInitializer\
                .committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .set_group_id(second_screen_consumer_group_id_1)\
            .set_topics(topic_to_consume_from) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_props)\
            .build()

print(f"Start reading data from kafka topic '{topic_to_consume_from}' to create "
        f"Cassandra tables\n"
        "T6_b: top_bot_contributions_by_month, T6_h: top_human_contributors_by_month,\n"
        "T7_b: number_of_pull_requests_by_bots, T7_h: number_of_pull_requests_by_humans,\n"
        "T8_b: number_of_events_by_bots, T8_h: number_of_events_by_humans")

raw_events_ds_1 = env.from_source( source=kafka_consumer_second_screen_source_1, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="kafka_source")\


second_screen_consumer_group_id_2 = 'second_screen_consumer_group_id_2'


kafka_consumer_second_screen_source_2 = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_starting_offsets(KafkaOffsetsInitializer\
                .committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .set_group_id(second_screen_consumer_group_id_2)\
            .set_topics(topic_to_consume_from) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_props)\
            .build()

raw_events_ds_2 = env.from_source( source=kafka_consumer_second_screen_source_2, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="kafka_source")\



second_screen_consumer_group_id_3 = 'second_screen_consumer_group_id_3'


kafka_consumer_second_screen_source_3 = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_starting_offsets(KafkaOffsetsInitializer\
                .committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .set_group_id(second_screen_consumer_group_id_3)\
            .set_topics(topic_to_consume_from) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_props)\
            .build()

raw_events_ds_3 = env.from_source( source=kafka_consumer_second_screen_source_3, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="kafka_source")\


#endregion

# V. Transform the original datastream, extract fields and store into Cassandra tables
#region 

max_concurrent_requests = 1000

# Q8_h: Number of events by humans
# region

# Q8_h_1. Transform the original stream 
def filter_out_bot_events_q8_h(eventString):
    '''
    Exclude bot events
    '''
    # event_dict = json.loads(eventString)
    event_dict = eval(eventString)

    # Keep only human events
    is_human = False
    # Exclude bot events
    username = event_dict['actor']
    if not username.endswith('[bot]'):
        is_human = True
        
    # Keep events only if created by humans
    if is_human:
        return True   
    
    
# Extract the number of events made by humans per type and month
def extract_number_of_human_events_per_type_and_create_row_q8_h(eventString):
    
    event_dict = eval(eventString)
    # event_dict = json.loads(eventString)
    
    # Extract [month, event_type, number_of_events]
    # month
    created_at = event_dict["created_at"]
    created_at_full_datetime = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
    created_at_year_month_only = datetime.strftime(created_at_full_datetime, "%Y-%m")
    month = created_at_year_month_only
    
    # event_type 
    event_type = event_dict["type"]
    
    # number_of_events
    number_of_events = 1
    
    number_of_human_events_per_type_by_month_info_row = Row(number_of_events, event_type, month)
    return number_of_human_events_per_type_by_month_info_row



# Type info for number of pull requests by bots by month
number_of_human_events_per_type_by_month_type_info_q8_h = \
    Types.ROW_NAMED(['number_of_events', 'event_type', 'month'], \
    [Types.LONG(), Types.STRING(), \
        Types.STRING()])
    
    

window_func_output_typing = Types.LIST(number_of_human_events_per_type_by_month_type_info_q8_h)

class RecordToListWindowFunction(ProcessAllWindowFunction[tuple, List[tuple], CountWindow]):
    def process(self, window: CountWindow, inputs: Iterable[tuple]):
        result = list(inputs)
        yield result

# # Datastream with extracted fields
# number_of_events_info_ds_q8_h = raw_events_ds_3.filter(filter_out_bot_events_q8_h) \
#                     .map(extract_number_of_human_events_per_type_and_create_row_q8_h, \
#                            output_type=number_of_human_events_per_type_by_month_type_info_q8_h)\
#                     # .window_all(CountTumblingWindowAssigner.of(window_size=2)) \
#                     # .process(RecordToListWindowFunction(), window_func_output_typing)
 

# TODO: Insert the data into the cassandra table through execute_concurrent_with_args()
# See if you can pass the session in a process function to use the execute_concurrent_with_args for multiple records (e.g. 10.000)
# This does not run (somehow)



# # Q8_h_2. Create Cassandra table number_of_human_events_per_type_by_month and sink data into it

# Create the table if not exists
create_number_of_pull_requests_by_humans_q8_h = \
    "CREATE TABLE IF NOT EXISTS prod_gharchive.number_of_human_events_per_type_by_month "\
    "(month text, event_type text, number_of_events counter, PRIMARY KEY ((month), "\
    "event_type)) WITH CLUSTERING ORDER BY "\
    "(event_type ASC);"
session.execute(create_number_of_pull_requests_by_humans_q8_h)


# Upsert query to be executed for every element
upsert_element_into_number_of_human_events_per_type_by_month_q8_h = \
            "UPDATE prod_gharchive.number_of_human_events_per_type_by_month "\
            "SET number_of_events = number_of_events + ? WHERE "\
            "event_type = ? AND month = ?;"

num_of_elements_per_window = 50

import time 

from cassandra.concurrent import execute_concurrent_with_args

# Child class of ProcessFunction
class RecordToListWindowFunction(ProcessAllWindowFunction):    
    # global num_of_elements_per_window
    def open(self, runtime_context):
        self.cluster = Cluster(['cassandra'], port=9142)
        self.session = self.cluster.connect()
        self.session.set_keyspace("prod_gharchive")
        self.prepared_update_stmt = self.session.prepare(
        "UPDATE prod_gharchive.number_of_human_events_per_type_by_month "\
            "SET number_of_events = number_of_events + ? WHERE "\
            "event_type = ? AND month = ?;"
    )
    
    def process(self, context: ProcessAllWindowFunction.Context, elements: Iterable[tuple]):
        result = list(elements)
        # print(f"New list through the process function: {result}")


        parameters_lists = list(result)
        # print(f"type of parameters: {type(parameters)}")
        # print(parameters)
        # time.sleep(1)
        # print(f"Inserting into cassansdra: {parameters_lists} ...")
        successes_and_failures = execute_concurrent_with_args(session=self.session, statement=self.prepared_update_stmt, parameters=parameters_lists, concurrency=1000)

        # # If failures occured:
        # if failures:
        #     raise Exception("Operation failed, failures occured")

        yield result


    def close(self):
        # Close the cassandra connection
        self.session.shutdown()
        self.cluster.shutdown()
        

number_of_events_info_ds_q8_h = raw_events_ds_3.filter(filter_out_bot_events_q8_h) \
                    .map(extract_number_of_human_events_per_type_and_create_row_q8_h, \
                           output_type=number_of_human_events_per_type_by_month_type_info_q8_h)\
                    .window_all(CountTumblingWindowAssigner.of(window_size=1000))\
                    .process(RecordToListWindowFunction(), window_func_output_typing)



# endregion

# endregion


if __name__ =='__main__':
    
    

    # Execute the flink streaming environment
    env.execute(os.path.splitext(os.path.basename(__file__))[0])

