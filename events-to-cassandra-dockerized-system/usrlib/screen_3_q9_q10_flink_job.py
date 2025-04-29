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
env.disable_operator_chaining()
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


# IV. Consume the original datastream 'near-real-time-raw-events'
#region 
kafka_props = {'enable.auto.commit': 'true',
               'auto.commit.interval.ms': '1000',
               'auto.offset.reset': 'smallest'}
def map_event_string_to_event_dict(event_string):
    return eval(event_string)

# Consume all events
screen_3_all_events_consumer_group_id = 'screen_3_all_events_consumer_group_id'
all_events_topic = "all_events"
all_events_source = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_starting_offsets(KafkaOffsetsInitializer\
                .committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .set_group_id(screen_3_all_events_consumer_group_id)\
            .set_topics(all_events_topic) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_props)\
            .build()
all_events_ds = env.from_source( source=all_events_source, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="all_events_source")\
            .map(map_event_string_to_event_dict)

# Consume push events
screen_3_push_events_consumer_group_id_1 = "screen_3_push_consumer_group_id_1"
push_events_topic = "push_events_topic"    
push_events_source = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_starting_offsets(KafkaOffsetsInitializer\
                .committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .set_group_id(screen_3_push_events_consumer_group_id_1)\
            .set_topics(push_events_topic) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_props)\
            .build()
push_events_ds = env.from_source(source=push_events_source, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="push_events_source")\
            .map(map_event_string_to_event_dict)
            
# Consume pull request events
screen_3_pull_request_events_consumer_group_id = "screen_3_pull_request_consumer_group_id"
pull_request_events_topic = "pull_request_events_topic"    
pull_request_events_source = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_starting_offsets(KafkaOffsetsInitializer\
                .committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .set_group_id(screen_3_pull_request_events_consumer_group_id)\
            .set_topics(pull_request_events_topic) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_props)\
            .build()
pull_request_events_ds = env.from_source(source=pull_request_events_source, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="pull_request_events_source")\
            .map(map_event_string_to_event_dict)
            
#endregion

# V. Transform the original datastream, extract fields and store into Cassandra tables
#region 

cassandra_host = 'cassandra_stelios'
cassandra_port = 9142
max_concurrent_requests = 1000
print(f"Start reading data from kafka topics to create "
        f"Cassandra tables:\n"
        "T9: stars_per_day_on_js_repo, T10: top_contributors_of_js_repo\n")
        
        
# Q9: Total stars by day on Javascript repo
# region

def keep_js_repos_events(event_dict):
    js_repos_list = ['marko-js/marko', 'mithriljs/mithril.js', 'angular/angular', 
        'angular/angular.js', 'emberjs/ember.js', 'knockout/knockout', 'tastejs/todomvc',
        'spine/spine', 'vuejs/vue', 'vuejs/core', 'Polymer/polymer', 'facebook/react', 
        'finom/seemple', 'aurelia/framework', 'optimizely/nuclear-js', 'jashkenas/backbone', 
        'dojo/dojo', 'jorgebucaran/hyperapp', 'riot/riot', 'daemonite/material', 
        'polymer/lit-element', 'aurelia/aurelia', 'sveltejs/svelte', 'neomjs/neo', 
        'preactjs/preact', 'hotwired/stimulus', 'alpinejs/alpine', 'solidjs/solid', 
        'ionic-team/stencil', 'jquery/jquery']
    if event_dict["repo"] in js_repos_list:
        return True
    else:
        return False
    
def keep_star_events(event_dict):
    if event_dict["type"] == "WatchEvent":
        return True
    else:
        return False

def create_row_q9(event_dict):
    number_of_stars = 1
    return Row(event_dict["number_of_stars": number_of_stars,
                          "repo": event_dict["repo"],
                          "day": event_dict["day"]])    


number_of_stars_on_js_repo_by_day_type_info_q9 = \
    Types.ROW_NAMED(['number_of_stars', 'repo', 'day'], \
    [Types.LONG(), Types.STRING(), \
        Types.STRING()])
number_of_stars_of_js_repo_by_day_info_ds_q9 = all_events_ds.filter(keep_js_repos_events)\
                    .filter(keep_star_events)\
                    .map(create_row_q9, \
                    output_type=number_of_stars_on_js_repo_by_day_type_info_q9)
upsert_element_into_number_of_stars_of_js_repo_per_day_q9 = \
            "UPDATE prod_gharchive.stars_per_day_on_js_repo "\
            "SET number_of_stars = number_of_stars + ? WHERE "\
            "repo_name = ? AND day = ?;"
cassandra_sink_q9 = CassandraSink.add_sink(number_of_stars_of_js_repo_by_day_info_ds_q9)\
    .set_query(upsert_element_into_number_of_stars_of_js_repo_per_day_q9)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()

#endregion


# Q10: Top contributors of js repo by month
# region

def filter_out_non_contributing_pull_request_events(event_dict):
    # Pull requests should be merged
    if event_dict["action"] == "closed" and \
    event_dict["merged_at"] != None:
        return True
    else:
        return False

def create_row_q10(event_dict):
    full_day = event_dict["day"]
    full_day_to_datetime_object = datetime.strptime(full_day, "%Y-%m-%d")
    month_of_day = datetime.strftime(full_day_to_datetime_object, "%Y-%m")
    return Row(event_dict["number_of_contributions"],
               event_dict["repo"],
               month_of_day,
               event_dict["username"])

human_contributions_by_month_type_info_q10 = \
    Types.ROW_NAMED(['number_of_contributions', 'repo_name', 'month', 'username'], \
    [Types.LONG(), Types.STRING(), \
        Types.STRING(), Types.STRING()])

pull_request_contributors_of_js_repos_ds_q10 = pull_request_events_ds\
    .filter(keep_js_repos_events)\
    .filter(filter_out_non_contributing_pull_request_events)
push_contributors_of_js_repos_ds_q10 = push_events_ds\
    .filter(keep_js_repos_events)
all_contributors_of_js_repos_ds = pull_request_contributors_of_js_repos_ds_q10\
    .union(push_contributors_of_js_repos_ds_q10)\
    .map(create_row_q10,\
        output_type=human_contributions_by_month_type_info_q10)

upsert_element_into_top_contributors_of_js_repo_q10 = \
            "UPDATE prod_gharchive.top_contributors_of_js_repo "\
            "SET number_of_contributions = number_of_contributions + ? WHERE "\
            "repo_name = ? AND month = ? AND username = ?;"
cassandra_sink_q10 = CassandraSink.add_sink(all_contributors_of_js_repos_ds)\
    .set_query(upsert_element_into_top_contributors_of_js_repo_q10)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()

# endregion

# endregion



if __name__ == '__main__':
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

        
    # Screen 3
    create_stars_per_day_on_js_repo_table_q9 = \
    "CREATE TABLE IF NOT EXISTS prod_gharchive.stars_per_day_on_js_repo "\
    "(day text, repo_name text, number_of_stars counter, PRIMARY KEY ((day, "\
    "repo_name)));"
    session.execute(create_stars_per_day_on_js_repo_table_q9)

    # Create the table if not exists
    create_top_contributors_of_js_repo_table_q10 = \
        "CREATE TABLE IF NOT EXISTS prod_gharchive.top_contributors_of_js_repo "\
        "(month text, username text, repo_name text, number_of_contributions counter, PRIMARY KEY ((repo_name, month), "\
        "username)) WITH CLUSTERING ORDER BY "\
        "(username ASC);"
    session.execute(create_top_contributors_of_js_repo_table_q10)
            
    cluster.shutdown()
    
    # Execute the flink streaming environment
    env.execute(os.path.splitext(os.path.basename(__file__))[0])

