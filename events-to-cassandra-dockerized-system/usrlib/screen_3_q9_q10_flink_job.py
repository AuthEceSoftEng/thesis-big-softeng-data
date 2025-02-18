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
import socket

# Note: Sections I-IV are used by all the transformed datastreams-to-table processes

# I. Set up the flink execution environment
# region 
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
# env.set_parallelism(2)

env.add_jars("file:///opt/flink/opt/flink-sql-connector-kafka-3.0.2-1.18.jar")
env.add_jars("file:///opt/flink/opt/flink-connector-cassandra_2.12-3.2.0-1.18.jar")
# env.add_jars("file:///opt/flink/opt/flink-streaming-scala_2.12-1.18.1.jar")

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
cassandra_port = 9042
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

# IV. Consume the original datastream 'near-real-time-raw-events'
#region 

kafka_props = {'enable.auto.commit': 'true',
               'auto.commit.interval.ms': '1000',
               'auto.offset.reset': 'smallest'}

third_screen_consumer_group_id = 'third_screen_consumer_group_id'

topic_to_consume_from = "historical-raw-events"
kafka_consumer_third_source = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_starting_offsets(KafkaOffsetsInitializer\
                .committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .set_group_id(third_screen_consumer_group_id)\
            .set_topics(topic_to_consume_from) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_props)\
            .build()

print(f"Start reading data from kafka topic '{topic_to_consume_from}' to create "
        f"Cassandra tables\n"
        "T9: stars_per_month_on_js_repo, T10: top_contributors_of_js_repo\n")
        
raw_events_ds = env.from_source( source=kafka_consumer_third_source, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="kafka_source")\

#endregion

# V. Transform the original datastream, extract fields and store into Cassandra tables
#region 

max_concurrent_requests = 100
# Q9: Total stars per month on Javascript repo
# region

# Q9_1. Transform the original stream 
# region

# Event types for stars:
event_types_with_info_q9 = ["WatchEvent"]
js_repos_list = ['marko-js/marko', 'mithriljs/mithril.js', 'angular/angular', 
    'angular/angular.js', 'emberjs/ember.js', 'knockout/knockout', 'tastejs/todomvc',
    'spine/spine', 'vuejs/vue', 'vuejs/core', 'Polymer/polymer', 'facebook/react', 
    'finom/seemple', 'aurelia/framework', 'optimizely/nuclear-js', 'jashkenas/backbone', 
    'dojo/dojo', 'jorgebucaran/hyperapp', 'riot/riot', 'daemonite/material', 
    'polymer/lit-element', 'aurelia/aurelia', 'sveltejs/svelte', 'neomjs/neo', 
    'preactjs/preact', 'hotwired/stimulus', 'alpinejs/alpine', 'solidjs/solid', 
    'ionic-team/stencil', 'jquery/jquery']

# Filter out events of type that contain no info we need
def filter_out_non_star_events_and_non_js_repos_q9(eventString):
    '''
    Keep only WatchEvents (meaning stars) 
    and also exclude non js repo events
    '''

    # Turn the json event object into event into a dict
    global event_types_with_info_q9     
    # eventDict = json.loads(eventString)
    eventDict = eval(eventString)

    # Keep only WatchEvents
    event_type = eventDict["type"]
    if (event_type != "WatchEvent"):
        is_watch_event = False
    else:
        is_watch_event = True
    
    # Keep only events on the list of js_repos
    is_js_repo = False
    repo_name = eventDict["repo"]["full_name"]
    global js_repos_list
    
    if repo_name in js_repos_list:
        is_js_repo = True
        
    # Keep watch events only made on js repos
    if is_watch_event and is_js_repo:
        return True
       
    
# Extract the number of stars in the events
def extract_stars_on_js_repo_and_create_row_q9(eventString):
    
    eventDict = eval(eventString)
    # eventDict = json.loads(eventString)
    
    # Extract [month, username, number_of_contributions]
    # month
    created_at = eventDict["created_at"]
    created_at_full_datetime = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
    created_at_year_month_only = datetime.strftime(created_at_full_datetime, "%Y-%m")
    month = created_at_year_month_only
    
    # repo_name
    repo_name = eventDict["repo"]["full_name"]
    
    # number_of_stars 
    number_of_stars = 1
    
    stars_per_month_on_js_repo_info_row = Row(number_of_stars, repo_name, month)
    return stars_per_month_on_js_repo_info_row




# Type info for number of stars of js repo by month
number_of_stars_on_js_repo_by_month_type_info_q9 = \
    Types.ROW_NAMED(['number_of_stars', 'username', 'month'], \
    [Types.LONG(), Types.STRING(), \
        Types.STRING()])
    
# Datastream with extracted fields
number_of_stars_of_js_repo_by_month_info_ds_q9 = raw_events_ds.filter(filter_out_non_star_events_and_non_js_repos_q9)\
                    .map(extract_stars_on_js_repo_and_create_row_q9, \
                           output_type=number_of_stars_on_js_repo_by_month_type_info_q9) \



#endregion

# Q9_2. Create Cassandra table and sink data into it
#region 

# Create the table if not exists
create_stars_per_month_on_js_repo_table_q9 = \
    "CREATE TABLE IF NOT EXISTS prod_gharchive.stars_per_month_on_js_repo "\
    "(month text, repo_name text, number_of_stars counter, PRIMARY KEY ((month, "\
    "repo_name)));"
session.execute(create_stars_per_month_on_js_repo_table_q9)


# Upsert query to be executed for every element
upsert_element_into_number_of_stars_of_js_repo_per_month_q9 = \
            "UPDATE prod_gharchive.stars_per_month_on_js_repo "\
            "SET number_of_stars = number_of_stars + ? WHERE "\
            "repo_name = ? AND month = ?;"


cassandra_sink_builder_q9 = CassandraSink.add_sink(number_of_stars_of_js_repo_by_month_info_ds_q9)\
    # .set_cluster_builder(cassandra_cluster_builder)
cassandra_sink_q9 = cassandra_sink_builder_q9.set_query(upsert_element_into_number_of_stars_of_js_repo_per_month_q9)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .enable_ignore_null_fields()\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .build()


# number_of_stars_of_js_repo_by_month_info_ds_q9.print()

#endregion

# endregion

# Q10: Top contributors of js repo by month
# region

# Q10_1. Transform the original stream 
# region

# Event types where the needed fields reside:
event_types_with_info_q10 = ["PushEvent", "PullRequestEvent"]

# Filter out events of type that contain no info we need
def filter_out_non_contributing_events_and_non_js_repos_q10(eventString):
    '''
    Keep only PushEvents and closed PullRequestEvents (meaning merged)
    and also exclude non js repo events
    '''

    # Turn the json event object into event into a dict
    global event_types_with_info_q10
    # eventDict = json.loads(eventString)
    eventDict = eval(eventString)

    # Keep only Push and merged PullRequest events
    is_push_or_merged_pull_request_event = False
    event_type = eventDict["type"]
    if (event_type == "PushEvent") or \
    (event_type == "PullRequestEvent" and \
    eventDict["payload"]["action"] == "closed" and \
    eventDict["payload"]["pull_request"]["merged_at"] != None):
        is_push_or_merged_pull_request_event = True
    else:
        is_push_or_merged_pull_request_event = False
    
    # Keep only events on the list of js_repos
    is_js_repo = False
    repo_name = eventDict["repo"]["full_name"]
    global js_repos_list
    if repo_name in js_repos_list:
        is_js_repo = True
    
    # Keep push and pull_request events only made on js repos
    if is_push_or_merged_pull_request_event and is_js_repo:
        return True
    
    
        
# Extract the number of stars in the events
def extract_top_contributors_on_js_repo_and_create_row_q10(eventString):
    
    eventDict = eval(eventString)
    # eventDict = json.loads(eventString)
    
    # Extract [number_of_contributions, repo_name, month, username]
    # month
    created_at = eventDict["created_at"]
    created_at_full_datetime = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
    created_at_year_month_only = datetime.strftime(created_at_full_datetime, "%Y-%m")
    month = created_at_year_month_only
    
    # repo_name
    repo_name = eventDict["repo"]["full_name"]
    
    # number of contributions 
    # (It equals the number of commits of a push  
    # or the number of commits of a merged pull-request)
    event_type = eventDict["type"]
    number_of_contributions = 0
    if event_type == "PushEvent":
        username = eventDict["actor"]
        number_of_contributions = eventDict["payload"]["distinct_size"]
    elif event_type == "PullRequestEvent":
        username = eventDict["payload"]["pull_request"]["user"]
        number_of_contributions = eventDict["payload"]["pull_request"]["commits"]
    
    top_contributors_of_js_repo_info_row = \
        Row(number_of_contributions, repo_name, month, username)
    return top_contributors_of_js_repo_info_row



# Type info for top contributors by month
human_contributions_by_month_type_info_q10 = \
    Types.ROW_NAMED(['number_of_contributions', 'repo_name', 'username', 'month'], \
    [Types.LONG(), Types.STRING(), \
        Types.STRING(), Types.STRING()])
    
# Datastream with extracted fields
top_contributors_of_js_repo_ds_q10 = raw_events_ds.filter(filter_out_non_contributing_events_and_non_js_repos_q10)\
                    .map(extract_top_contributors_on_js_repo_and_create_row_q10, \
                           output_type=human_contributions_by_month_type_info_q10)

#endregion


# Q10_2. Create Cassandra table and sink data into it
#region 

# Create the table if not exists
create_top_contributors_of_js_repo_table_q10 = \
    "CREATE TABLE IF NOT EXISTS prod_gharchive.top_contributors_of_js_repo "\
    "(month text, username text, repo_name text, number_of_contributions counter, PRIMARY KEY ((repo_name, month), "\
    "username)) WITH CLUSTERING ORDER BY "\
    "(username ASC);"
session.execute(create_top_contributors_of_js_repo_table_q10)


# Upsert query to be executed for every element
upsert_element_into_top_contributors_of_js_repo_q10 = \
            "UPDATE prod_gharchive.top_contributors_of_js_repo "\
            "SET number_of_contributions = number_of_contributions + ? WHERE "\
            "repo_name = ? AND month = ? AND username = ?;"



cassandra_sink_builder_q10 = CassandraSink.add_sink(top_contributors_of_js_repo_ds_q10)\
    # .set_cluster_builder(cassandra_cluster_builder)
cassandra_sink_q10 = cassandra_sink_builder_q10.set_query(upsert_element_into_top_contributors_of_js_repo_q10)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .enable_ignore_null_fields()\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .build()


# top_contributors_of_js_repo_ds_q10.print()

#endregion


# endregion



# endregion



if __name__ == '__main__':
    # Execute the flink streaming environment
    env.execute(os.path.splitext(os.path.basename(__file__))[0])

