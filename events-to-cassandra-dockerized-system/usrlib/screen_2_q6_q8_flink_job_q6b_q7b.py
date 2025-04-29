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

# Note: Sections I-IV are used by all the transformed datastreams-to-table processes

# I. Set up the flink execution environment
# region 
env = StreamExecutionEnvironment.get_execution_environment()
env.disable_operator_chaining()
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


# II. Get Kafka host and port to connect Flink to
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


# III. Consume datastreams
# region
kafka_props = {'enable.auto.commit': 'true',
               'auto.commit.interval.ms': '1000',
               'auto.offset.reset': 'smallest'}
def map_event_string_to_event_dict(event_string):
    return eval(event_string)

# Consume push events
screen_2_push_events_consumer_group_id = "screen_2_push_consumer_group_id"
push_events_topic = "push_events_topic"    
push_events_source = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_starting_offsets(KafkaOffsetsInitializer\
                .committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .set_group_id(screen_2_push_events_consumer_group_id)\
            .set_topics(push_events_topic) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_props)\
            .build()
push_events_ds = env.from_source(source=push_events_source, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="push_events_source")\
            .map(map_event_string_to_event_dict)

# Consume pull request events
screen_2_pull_request_events_consumer_group_id = "screen_2_pull_request_consumer_group_id"
pull_request_events_topic = "pull_request_events_topic"    
pull_request_events_source = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_starting_offsets(KafkaOffsetsInitializer\
                .committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .set_group_id(screen_2_pull_request_events_consumer_group_id)\
            .set_topics(pull_request_events_topic) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_props)\
            .build()
pull_request_events_ds = env.from_source(source=pull_request_events_source, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="pull_reequest_events_source")\
            .map(map_event_string_to_event_dict)
# endregion



# V. Transform the original datastream, extract fields and store into Cassandra tables
#region 

max_concurrent_requests = 1000
cassandra_host = 'cassandra_stelios'
cassandra_port = 9142
print(f"Insert data from kafka topics into Cassandra tables:\n"
        "T6_b: top_bot_contributions_by_day, T6_h: top_human_contributors_by_day,\n"
        "T7_b: number_of_pull_requests_by_bots")



    
# Q6_b: Top bot contributors by day
# region
def filter_out_human_events(event_dict):
    if not event_dict["username"].endswith('[bot]'):
        return True
    else:
        return False
    
def filter_out_bot_events(event_dict):
    if event_dict["username"].endswith('[bot]'):
        return True
    else:
        return False
    
def filter_out_irregular_push_events(event_dict):
    # Number of push contributions should be regular
    number_of_contributions = event_dict["number_of_contributions"]
    if (number_of_contributions <= 100) or \
    (number_of_contributions <= 200 and 
    event_dict["size"] == event_dict["distinct_size"]):
        return True
    else:
        return False
    
def filter_out_non_contributing_pull_request_events(event_dict):
    # Number of pull request contributions should be regular
    number_of_contributions = event_dict["number_of_contributions"]
    if number_of_contributions <= 200:
        is_num_of_contributions_regular = True    
    else:
        is_num_of_contributions_regular = False

    # Pull requests should be merged
    if event_dict["action"] == "closed" and \
    event_dict["merged_at"] != None:
        was_pull_request_merged = True
    else:
        was_pull_request_merged = False
    
    if is_num_of_contributions_regular == True and was_pull_request_merged == True:
        return True
    else:
        return False

def create_row_q6(event_dict):
    return Row(event_dict["number_of_contributions"], 
               event_dict["username"], 
               event_dict["day"])
    
contributing_push_events_ds = push_events_ds\
                .filter(filter_out_irregular_push_events)
contributing_pull_request_events_ds = pull_request_events_ds\
                .filter(filter_out_non_contributing_pull_request_events)
contributing_events_ds = contributing_push_events_ds.union(contributing_pull_request_events_ds)


contributions_by_day_type_info_q6b = \
    Types.ROW_NAMED(['number_of_contributions', 'username', 'day'], \
    [Types.LONG(), Types.STRING(), \
        Types.STRING()])
top_bot_contributors_info_ds_q6_b = contributing_events_ds\
                .filter(filter_out_human_events)\
                .map(create_row_q6, output_type=contributions_by_day_type_info_q6b)

upsert_element_into_top_bot_contributors_q6_b = \
            "UPDATE prod_gharchive.top_bot_contributors_by_day "\
            "SET number_of_contributions = number_of_contributions + ? WHERE "\
            "username = ? AND day = ?;"
cassandra_sink_q6_b = CassandraSink.add_sink(top_bot_contributors_info_ds_q6_b)\
    .set_query(upsert_element_into_top_bot_contributors_q6_b)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()
#endregion


# Q6_h: Top human contributors by day
# region
contributions_by_day_type_info_q6_h = contributions_by_day_type_info_q6b
top_human_contributors_info_ds_q6_h = contributing_events_ds\
                .filter(filter_out_bot_events)\
                .map(create_row_q6, output_type=contributions_by_day_type_info_q6_h)
                
upsert_element_into_top_human_contributors_q6_h = \
            "UPDATE prod_gharchive.top_human_contributors_by_day "\
            "SET number_of_contributions = number_of_contributions + ? WHERE "\
            "username = ? AND day = ?;"
cassandra_sink_q6_h = CassandraSink.add_sink(top_human_contributors_info_ds_q6_h)\
    .set_query(upsert_element_into_top_human_contributors_q6_h)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()
# endregion

# Q7_b: Number of pull requests by bots
# region

# Q7_b_1. Transform the original stream 
# Filter out events of type that contain no info we need
def filter_out_non_pull_request_events_q7_b(eventString):
    '''
    Keep only PullRequestEvents and also exclude human events
    '''

    # Turn the json event object into event into a dict
    event_types_with_info_q7_b = ["PullRequestEvent"]
    # event_dict = json.loads(eventString)
    event_dict = eval(eventString)

    # Keep only PullRequest events
    is_pull_request_event = False
    event_type = event_dict["type"]
    if (event_type == "PullRequestEvent" and \
    event_dict["payload"]["action"] == "closed"):
        is_pull_request_event = True
    else:
        is_pull_request_event = False
        return False
    
    # Keep only bot events
    # Exclude human events
    username = event_dict["payload"]["pull_request"]["user"]
    if username.endswith('[bot]'):
        is_bot = True
    else:
        is_bot = False
        return False
        
    # Keep push and merged pull-request events only if not created by bots
    if is_pull_request_event and is_bot:
        return True
    
def extract_number_of_pull_requests_and_create_row_q7_b(eventString):
    
    event_dict = eval(eventString)
    # event_dict = json.loads(eventString)
    
    # Extract [day, username, number_of_contributions]
    # day
    created_at = event_dict["created_at"]
    created_at_full_datetime = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
    created_at_year_month_day_only = datetime.strftime(created_at_full_datetime, "%Y-%m-%d")
    day = created_at_year_month_day_only
    
    # Number of pull requests
    number_of_pull_requests = 1
    if event_dict["payload"]["pull_request"]["merged_at"] != None:
        was_accepted = True
    else:
        was_accepted = False
    pull_requests_by_bots_info_row = Row(number_of_pull_requests, was_accepted, day)
    return pull_requests_by_bots_info_row

# Type info for number of pull requests by bots by day
number_of_pull_requests_by_bots_by_day_type_info_q7_b = \
    Types.ROW_NAMED(['number_of_pull_requests', 'was_accepted', 'day'], \
    [Types.LONG(),\
        Types.BOOLEAN(), Types.STRING()])


def filter_out_non_closed_pull_requests(event_dict):
    if event_dict["payload"]["action"] == "closed":
        return True
    else:
        return False

def create_row_q7(event_dict):
    number_of_pull_requests = 1
    if event_dict["payload"]["pull_request"]["merged_at"] != None:
        was_accepted = True
    else:
        was_accepted = False
    return Row(number_of_pull_requests, 
               was_accepted, 
               event_dict["day"])


number_of_pull_requests_ds_q7_b = pull_request_events_ds\
            .filter(filter_out_human_events)\
            .filter(filter_out_non_closed_pull_requests)\
            .map(create_row_q7, \
                    output_type=number_of_pull_requests_by_bots_by_day_type_info_q7_b) \

upsert_element_into_T7_b_number_of_pull_requests_by_bots = \
            "UPDATE prod_gharchive.number_of_pull_requests_by_bots "\
            "SET number_of_pull_requests = number_of_pull_requests + ? WHERE "\
            "was_accepted = ? AND day = ?;"
cassandra_sink_q7_b = CassandraSink.add_sink(number_of_pull_requests_ds_q7_b)\
    .set_query(upsert_element_into_T7_b_number_of_pull_requests_by_bots)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()
    
#endregion


# endregion


if __name__ =='__main__':
    
    # Create cassandra keyspace if not exist
    cassandra_host = 'cassandra_stelios'
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


    # Screen 2
    create_top_bot_contributors_table_q6_b = \
        "CREATE TABLE IF NOT EXISTS prod_gharchive.top_bot_contributors_by_day "\
        "(day text, username text, number_of_contributions counter, PRIMARY KEY ((day), "\
        "username)) WITH CLUSTERING ORDER BY "\
        "(username ASC);"
    session.execute(create_top_bot_contributors_table_q6_b)


    create_top_human_contributors_table_q6_h = \
        "CREATE TABLE IF NOT EXISTS prod_gharchive.top_human_contributors_by_day "\
        "(day text, username text, number_of_contributions counter, PRIMARY KEY ((day), "\
        "username)) WITH CLUSTERING ORDER BY "\
        "(username ASC);"
    session.execute(create_top_human_contributors_table_q6_h)

    create_number_of_pull_requests_by_bots_q7_b = \
        "CREATE TABLE IF NOT EXISTS prod_gharchive.number_of_pull_requests_by_bots "\
        "(day text, was_accepted boolean, number_of_pull_requests counter, PRIMARY KEY ((day, "\
        "was_accepted)));"
    session.execute(create_number_of_pull_requests_by_bots_q7_b)

    cluster.shutdown()
    
    # Execute the flink streaming environment
    env.execute(os.path.splitext(os.path.basename(__file__))[0])

