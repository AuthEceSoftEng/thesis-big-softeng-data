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
# env.disable_operator_chaining()
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
        "T6_b: top_bot_contributions_by_day, T6_h: top_human_contributors_by_day,\n"
        "T7_b: number_of_pull_requests_by_bots, T7_h: number_of_pull_requests_by_humans,\n"
        "T8_b: number_of_events_by_bots, T8_h: number_of_events_by_humans")

raw_events_ds = env.from_source( source=kafka_consumer_second_screen_source_1, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="kafka_source")\



#endregion

# V. Transform the original datastream, extract fields and store into Cassandra tables
#region 

max_concurrent_requests = 1000

# Q6_b: Top bot contributors by day
# region

# Q6_b_1. Transform the original stream 
# Filter out events of type that contain no info we need
def filter_out_non_contributing_events_and_humans_q6_b(eventString):
    '''
    Keep only PushEvents and closed PullRequestEvents (meaning merged) 
    and also exclude human events
    '''

    # Turn the json event object into event into a dict
    event_types_with_info_q6_b = ["PushEvent", "PullRequestEvent"]
    # event_dict = json.loads(eventString)
    event_dict = eval(eventString)

    # Keep only Push and merged PullRequest events
    event_type = event_dict["type"]
    if (event_type == "PushEvent"):
        is_push_or_merged_pull_request_event = True
    elif (event_type == "PullRequestEvent" and \
    event_dict["payload"]["action"] == "closed" and \
    event_dict["payload"]["pull_request"]["merged_at"] != None):
        is_push_or_merged_pull_request_event = True
    else:
        return False
    
    # Keep PullRequestEvents if number_of_contributions <= 200
    # Keep PushEvents if number_of_contributions <= 200 or if size == distinct_size
    is_num_of_contributions_regular = False
    if event_type == "PushEvent":
        number_of_contributions = event_dict["payload"]["distinct_size"]
        if (number_of_contributions <= 100) or (number_of_contributions <= 200
        and event_dict["payload"]["size"] == event_dict["payload"]["distinct_size"]):
                is_num_of_contributions_regular = True
    elif event_type == "PullRequestEvent":
        number_of_contributions = event_dict["payload"]["pull_request"]["commits"]
        if number_of_contributions <= 200:
            is_num_of_contributions_regular = True
    else:
        return False

    # Keep only bot events
    if event_type == "PushEvent":
        username = event_dict["actor"]
    elif event_type == "PullRequestEvent":
        username = event_dict["payload"]["pull_request"]["user"]
    
    if username.endswith('[bot]'):
        is_bot = True
    else:
        return False


    if is_push_or_merged_pull_request_event and is_num_of_contributions_regular and is_bot:
        return True
    
# Extract the number of events
def extract_number_of_contributions_and_create_row_q6_b(eventString):
    
    event_dict = eval(eventString)
    
    # Extract [day, username, number_of_contributions]
    # day
    created_at = event_dict["created_at"]
    created_at_full_datetime = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
    created_at_year_month_day_only = datetime.strftime(created_at_full_datetime, "%Y-%m-%d")
    day = created_at_year_month_day_only
        
    
    # 'username' and 'number of contributions' 
    # (The number of contributions equals the number of commits of a push  
    # or the number of commits of a merged pull-request)
    event_type = event_dict["type"]
    number_of_contributions = 0
    if event_type == "PushEvent":
        username = event_dict["actor"]
        number_of_contributions = event_dict["payload"]["distinct_size"]
    elif event_type == "PullRequestEvent":
        username = event_dict["payload"]["pull_request"]["user"]
        number_of_contributions = event_dict["payload"]["pull_request"]["commits"]
    
    bots_contributions_info_row = Row(number_of_contributions, username, day)
    return bots_contributions_info_row

# Type info for bot contributions by day
bot_contributions_by_day_type_info = \
    Types.ROW_NAMED(['number_of_contributions', 'username', 'day'], \
    [Types.LONG(), Types.STRING(), \
        Types.STRING()])
    
# Datastream with extracted fields
top_bot_contributors_info_ds_q6_b = raw_events_ds.filter(filter_out_non_contributing_events_and_humans_q6_b)\
                    .map(extract_number_of_contributions_and_create_row_q6_b, \
                           output_type=bot_contributions_by_day_type_info) \
# Uncomment to print datastream
# top_bot_contributors_info_ds_q6_b.print()

# Q6_b_2. Create Cassandra table sink data into it
# Create the table if not exists
create_top_bot_contributors_table_q6_b = \
    "CREATE TABLE IF NOT EXISTS prod_gharchive.top_bot_contributors_by_day "\
    "(day text, username text, number_of_contributions counter, PRIMARY KEY ((day), "\
    "username)) WITH CLUSTERING ORDER BY "\
    "(username ASC);"
session.execute(create_top_bot_contributors_table_q6_b)

# Upsert query to be executed for every element
upsert_element_into_top_bot_contributors_q6_b = \
            "UPDATE prod_gharchive.top_bot_contributors_by_day "\
            "SET number_of_contributions = number_of_contributions + ? WHERE "\
            "username = ? AND day = ?;"

# Sink events into the Cassandra table 
cassandra_sink_q6_b = CassandraSink.add_sink(top_bot_contributors_info_ds_q6_b)\
    .set_query(upsert_element_into_top_bot_contributors_q6_b)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()
    

#endregion


# Q6_h: Top human contributors by day
# region

# Q6_h_1. Transform the original stream 
# Filter out events of type that contain no info we need
def filter_out_non_contributing_events_and_bots_q6_h(eventString):
    '''
    Keep only PushEvents and closed PullRequestEvents (meaning merged) 
    and also exclude bot events
    '''

    # Event types where the needed fields reside:
    event_types_with_info_q6_h = ["PushEvent", "PullRequestEvent"]

    # event_dict = json.loads(eventString)
    event_dict = eval(eventString)

    # Keep only Push and merged PullRequest events
    event_type = event_dict["type"]
    if (event_type == "PushEvent"):
        is_push_or_merged_pull_request_event = True
    elif (event_type == "PullRequestEvent" and \
    event_dict["payload"]["action"] == "closed" and \
    event_dict["payload"]["pull_request"]["merged_at"] != None):
        is_push_or_merged_pull_request_event = True
    else:
        return False
    
    
    is_num_of_contributions_regular = False
    # Keep PullRequestEvents if number_of_contributions <= 200
    # Keep PushEvents if number_of_contributions <= 200 or if size == distinct_size
    if event_type == "PushEvent":
        number_of_contributions = event_dict["payload"]["distinct_size"]
        if (number_of_contributions <= 100) or (number_of_contributions <= 200
        and event_dict["payload"]["size"] == event_dict["payload"]["distinct_size"]):
            is_num_of_contributions_regular = True
    elif event_type == "PullRequestEvent":
        number_of_contributions = event_dict["payload"]["pull_request"]["commits"]
        if number_of_contributions <= 200:
            is_num_of_contributions_regular = True
    else:
        return False

    # Keep only human events
    if event_type == "PushEvent":
        username = event_dict["actor"]
    elif event_type == "PullRequestEvent":
        username = event_dict["payload"]["pull_request"]["user"]
    
    if not username.endswith('[bot]'):
        is_human = True
    else: 
        return False
        
    # Keep push and merged pull-request events only if not created by bots
    if is_push_or_merged_pull_request_event and is_human and is_num_of_contributions_regular:
        return True
    
# Extract the number of events
def extract_number_of_contributions_and_create_row_q6_h(eventString):
    
    event_dict = eval(eventString)
    
    # Extract [day, username, number_of_contributions]
    # day
    created_at = event_dict["created_at"]
    created_at_full_datetime = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
    created_at_year_month_day_only = datetime.strftime(created_at_full_datetime, "%Y-%m-%d")
    day = created_at_year_month_day_only
        
    # username and number of contributions 
    # (It equals the number of commits of a push  
    # or the number of commits of a merged pull-request)
    event_type = event_dict["type"]
    number_of_contributions = 0
    if event_type == "PushEvent":
        username = event_dict["actor"]
        number_of_contributions = event_dict["payload"]["distinct_size"]
    elif event_type == "PullRequestEvent":
        username = event_dict["payload"]["pull_request"]["user"]
        number_of_contributions = event_dict["payload"]["pull_request"]["commits"]
    humans_contributions_info_row = Row(number_of_contributions, username, day)
    return humans_contributions_info_row

output_type_of_process = [Types.LONG(), Types.STRING(), Types.STRING()]

# Type info for human contributions by day
human_contributions_by_day_type_info_q6_h = \
    Types.ROW_NAMED(['number_of_contributions', 'username', 'day'], \
    [Types.LONG(), Types.STRING(), \
        Types.STRING()])
    
# Datastream with extracted fields
top_human_contributors_info_ds_q6_h = raw_events_ds.filter(filter_out_non_contributing_events_and_bots_q6_h)\
                    .map(extract_number_of_contributions_and_create_row_q6_h, \
                           output_type=human_contributions_by_day_type_info_q6_h) \
# Uncomment to print datastream
# top_human_contributors_info_ds_q6_h.print()

# Q6_h_2. Create Cassandra table and sink data into it
# Create the table if not exists
create_top_human_contributors_table_q6_h = \
    "CREATE TABLE IF NOT EXISTS prod_gharchive.top_human_contributors_by_day "\
    "(day text, username text, number_of_contributions counter, PRIMARY KEY ((day), "\
    "username)) WITH CLUSTERING ORDER BY "\
    "(username ASC);"
session.execute(create_top_human_contributors_table_q6_h)

# Upsert query to be executed for every element
upsert_element_into_top_human_contributors_q6_h = \
            "UPDATE prod_gharchive.top_human_contributors_by_day "\
            "SET number_of_contributions = number_of_contributions + ? WHERE "\
            "username = ? AND day = ?;"

# Sink events into the Cassandra table 
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
        were_accepted = True
    else:
        were_accepted = False
    pull_requests_by_bots_info_row = Row(number_of_pull_requests, were_accepted, day)
    return pull_requests_by_bots_info_row

# Type info for number of pull requests by bots by day
number_of_pull_requests_by_bots_by_day_type_info_q7_b = \
    Types.ROW_NAMED(['number_of_pull_requests', 'were_accepted', 'day'], \
    [Types.LONG(),\
        Types.BOOLEAN(), Types.STRING()])
    
# Datastream with extracted fields
number_of_pull_requests_info_ds_q7_b = raw_events_ds.filter(filter_out_non_pull_request_events_q7_b)\
                    .map(extract_number_of_pull_requests_and_create_row_q7_b, \
                           output_type=number_of_pull_requests_by_bots_by_day_type_info_q7_b) \
# Uncomment to print datastream
# number_of_pull_requests_info_ds_q7_b.print()


# Q7_b_2. Create Cassandra table and sink data into it
# Create the table if not exists
create_number_of_pull_requests_by_bots_q7_b = \
    "CREATE TABLE IF NOT EXISTS prod_gharchive.number_of_pull_requests_by_bots "\
    "(day text, were_accepted boolean, number_of_pull_requests counter, PRIMARY KEY ((day, "\
    "were_accepted)));"
session.execute(create_number_of_pull_requests_by_bots_q7_b)

# Upsert query to be executed for every element
upsert_element_into_T7_b_number_of_pull_requests_by_bots = \
            "UPDATE prod_gharchive.number_of_pull_requests_by_bots "\
            "SET number_of_pull_requests = number_of_pull_requests + ? WHERE "\
            "were_accepted = ? AND day = ?;"

# Sink events into the Cassandra table 
cassandra_sink_q7_b = CassandraSink.add_sink(number_of_pull_requests_info_ds_q7_b)\
    .set_query(upsert_element_into_T7_b_number_of_pull_requests_by_bots)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()
    
#endregion

# Q7_h: Number of pull requests by humans
# region

# Q7_h_1. Transform the original stream 
# Filter out events of type that contain no info we need
def filter_out_non_pull_request_events_q7_h(eventString):
    '''
    Keep only closing PullRequestEvents and also exclude bot events
    '''

    # Turn the json event object into event into a dict
    event_types_with_info_q7_h = ["PullRequestEvent"]
    
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
    
    # Keep only human events
    is_human = False
    username = None
    if (event_type == "PullRequestEvent"):
        username = event_dict['payload']['pull_request']['user']
        # Exclude bot events
        if not username.endswith('[bot]'):
            is_human = True
        
    # Keep push and merged pull-request events only if not created by bots
    if is_pull_request_event and is_human:
        return True
    
    
# Extract the number of events
def extract_number_of_pull_requests_and_create_row_q7_h(eventString):
    
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
        were_accepted = True
    else:
        were_accepted = False    
    pull_requests_by_humans_info_row = Row(number_of_pull_requests, were_accepted, day)
    return pull_requests_by_humans_info_row

# Type info for number of pull requests by bots by day
number_of_pull_requests_by_humans_by_day_type_info_q7_h = \
    Types.ROW_NAMED(['number_of_pull_requests', 'were_accepted', 'day'], \
    [Types.LONG(),\
        Types.BOOLEAN(), Types.STRING()])
    
# Datastream with extracted fields
number_of_pull_requests_info_ds_q7_h = raw_events_ds.filter(filter_out_non_pull_request_events_q7_h)\
                    .map(extract_number_of_pull_requests_and_create_row_q7_h, \
                           output_type=number_of_pull_requests_by_humans_by_day_type_info_q7_h) 
# Uncomment to print the datastream
# number_of_pull_requests_info_ds_q7_h.print()


# Q7_h_2. Create Cassandra table and sink data into it
# Create the table if not exists

create_number_of_pull_requests_by_humans_q7_h = \
    "CREATE TABLE IF NOT EXISTS prod_gharchive.number_of_pull_requests_by_humans "\
    "(day text, were_accepted boolean, number_of_pull_requests counter, PRIMARY KEY ((day, "\
    "were_accepted)));"
session.execute(create_number_of_pull_requests_by_humans_q7_h)

# Upsert query to be executed for every element
upsert_element_into_T7_h_number_of_pull_requests_by_humans = \
            "UPDATE prod_gharchive.number_of_pull_requests_by_humans "\
            "SET number_of_pull_requests = number_of_pull_requests + ? WHERE "\
            "were_accepted = ? AND day = ?;"

# Sink events into the Cassandra table 
cassandra_sink_q7_h = CassandraSink.add_sink(number_of_pull_requests_info_ds_q7_h)\
    .set_query(upsert_element_into_T7_h_number_of_pull_requests_by_humans)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()

# endregion


# Q8_b: Number of events by bots
# region

# Q8_b_1. Transform the original stream 
def filter_out_human_events_q8_b(eventString):
    '''
    Exclude human events
    '''
    # event_dict = json.loads(eventString)
    event_dict = eval(eventString)

    # Keep only bot events
    is_bot = False
    # Exclude human events
    username = event_dict['actor']
    if username.endswith('[bot]'):
        is_bot = True
        
    # Keep events only if created by bots
    if is_bot:
        return True   
    
# Extract the number of events made by humans per type and day
def extract_number_of_bot_events_per_type_and_create_row_q8_b(eventString):
    
    event_dict = eval(eventString)
    # event_dict = json.loads(eventString)
    
    # Extract [day, event_type, number_of_events]
    # day
    created_at = event_dict["created_at"]
    created_at_full_datetime = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
    created_at_year_month_day_only = datetime.strftime(created_at_full_datetime, "%Y-%m-%d")
    day = created_at_year_month_day_only
    
    # event_type 
    event_type = event_dict["type"]
    
    # number_of_events
    number_of_events = 1
    
    number_of_bot_events_per_type_by_day_info_row = Row(number_of_events, event_type, day)
    return number_of_bot_events_per_type_by_day_info_row




# Type info for number of pull requests by bots by day
number_of_bot_events_per_type_by_day_type_info_q8_b = \
    Types.ROW_NAMED(['number_of_events', 'event_type', 'day'], \
    [Types.LONG(), Types.STRING(), \
        Types.STRING()])
    
    
# Datastream with extracted fields
number_of_events_info_ds_q8_b = raw_events_ds.filter(filter_out_human_events_q8_b) \
                    .map(extract_number_of_bot_events_per_type_and_create_row_q8_b, \
                           output_type=number_of_bot_events_per_type_by_day_type_info_q8_b)
# Uncomment to print the datastream
# number_of_events_info_ds_q8_b.print()




# Q8_b_2. Create Cassandra table and sink data into it

# Create the table if not exists
create_number_of_pull_requests_by_bots_q8_b = \
    "CREATE TABLE IF NOT EXISTS prod_gharchive.number_of_bot_events_per_type_by_day "\
    "(day text, event_type text, number_of_events counter, PRIMARY KEY ((day), "\
    "event_type)) WITH CLUSTERING ORDER BY "\
    "(event_type ASC);"
session.execute(create_number_of_pull_requests_by_bots_q8_b)


# Upsert query to be executed for every element
upsert_element_into_number_of_bot_events_per_type_by_day_q8_b = \
            "UPDATE prod_gharchive.number_of_bot_events_per_type_by_day "\
            "SET number_of_events = number_of_events + ? WHERE "\
            "event_type = ? AND day = ?;"

# Sink events into the Cassandra table 
cassandra_sink_q8_b = CassandraSink.add_sink(number_of_events_info_ds_q8_b)\
    .set_query(upsert_element_into_number_of_bot_events_per_type_by_day_q8_b)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()

#endregion


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
    
    
# Extract the number of events made by humans per type and day
def extract_number_of_human_events_per_type_and_create_row_q8_h(eventString):
    
    event_dict = eval(eventString)
    # event_dict = json.loads(eventString)
    
    # Extract [day, event_type, number_of_events]
    # day
    created_at = event_dict["created_at"]
    created_at_full_datetime = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
    created_at_year_month_day_only = datetime.strftime(created_at_full_datetime, "%Y-%m-%d")
    day = created_at_year_month_day_only
    
    # event_type 
    event_type = event_dict["type"]
    
    # number_of_events
    number_of_events = 1
    
    number_of_human_events_per_type_by_day_info_row = Row(number_of_events, event_type, day)
    return number_of_human_events_per_type_by_day_info_row



# Type info for number of pull requests by bots by day
number_of_human_events_per_type_by_day_type_info_q8_h = \
    Types.ROW_NAMED(['number_of_events', 'event_type', 'day'], \
    [Types.LONG(), Types.STRING(), \
        Types.STRING()])
    
    
# Datastream with extracted fields
number_of_events_info_ds_q8_h = raw_events_ds.filter(filter_out_bot_events_q8_h) \
                    .map(extract_number_of_human_events_per_type_and_create_row_q8_h, \
                           output_type=number_of_human_events_per_type_by_day_type_info_q8_h)\
                    
# Uncomment to print the datastream elements
# number_of_events_info_ds_q8_h.print()


# Q8_h_2. Create Cassandra table number_of_human_events_per_type_by_day and sink data into it
# Create the table if not exists
create_number_of_pull_requests_by_humans_q8_h = \
    "CREATE TABLE IF NOT EXISTS prod_gharchive.number_of_human_events_per_type_by_day "\
    "(day text, event_type text, number_of_events counter, PRIMARY KEY ((day), "\
    "event_type)) WITH CLUSTERING ORDER BY "\
    "(event_type ASC);"
session.execute(create_number_of_pull_requests_by_humans_q8_h)

# Upsert query to be executed for every element
upsert_element_into_number_of_human_events_per_type_by_day_q8_h = \
            "UPDATE prod_gharchive.number_of_human_events_per_type_by_day "\
            "SET number_of_events = number_of_events + ? WHERE "\
            "event_type = ? AND day = ?;"

# Sink events into the Cassandra table 
cassandra_sink_q8_h = CassandraSink.add_sink(number_of_events_info_ds_q8_h)\
    .set_query(upsert_element_into_number_of_human_events_per_type_by_day_q8_h)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()

# endregion

# endregion


if __name__ =='__main__':
    
    

    # Execute the flink streaming environment
    env.execute(os.path.splitext(os.path.basename(__file__))[0])

