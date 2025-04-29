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

env.add_jars("file:///opt/flink/opt/flink-sql-connector-kafka-3.0.2-1.18.jar")
env.add_jars("file:///opt/flink/opt/flink-connector-cassandra_2.12-3.2.0-1.18.jar")
# env.add_jars("file:///opt/flink/opt/flink-streaming-scala_2.12-1.18.1.jar")
env.set_restart_strategy(RestartStrategies.\
    fixed_delay_restart(restart_attempts=3, delay_between_attempts=1000))

# endregion 

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


# III. Consume datastreams
#region 
kafka_props = {'enable.auto.commit': 'true',
               'auto.commit.interval.ms': '1000',
               'auto.offset.reset': 'smallest'}
def map_event_string_to_event_dict(event_string):
    return eval(event_string)

def create_kafka_source(kafka_bootstrap_servers=str, consumer_group_id=str, topic=str, kafka_properties=dict()):
    '''
    Returns a kafka source of offset reset strategy "EARLIEST" of given configs
    
    :param kafka_bootstrap_servers: Example value: 'kafka:9142'
    :param consumer_group_id: Example value: 'screen_2_push_consumer_group_id_1'
    :param topic: Example value: 'push_events'
    :param kafka_properties: {'enable.auto.commit': 'true',
               'auto.commit.interval.ms': '1000',
               'auto.offset.reset': 'smallest'}
    '''
    return KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_starting_offsets(KafkaOffsetsInitializer\
                .committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .set_group_id(consumer_group_id)\
            .set_topics(topic) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_properties)\
            .build()


# Consume pull request events
screen_4_pull_request_events_consumer_group_id = "screen_4_pull_request_consumer_group_id"
pull_request_events_topic = "pull_request_events_topic"    
pull_request_events_source = create_kafka_source(kafka_bootstrap_servers=kafka_bootstrap_servers,
                                         consumer_group_id=screen_4_pull_request_events_consumer_group_id,
                                         topic=pull_request_events_topic,
                                         kafka_properties=kafka_props)
pull_request_events_ds = env.from_source(source=pull_request_events_source, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="pull_request_events_source")\
            .map(map_event_string_to_event_dict)
     
# Consume issue events   
screen_4_issue_events_consumer_group_id = "screen_4_issue_consumer_group_id"
issue_events_topic = "issue_events_topic"    
issue_events_source = create_kafka_source(kafka_bootstrap_servers=kafka_bootstrap_servers,
                                         consumer_group_id=screen_4_issue_events_consumer_group_id,
                                         topic=issue_events_topic,
                                         kafka_properties=kafka_props)
issue_events_ds = env.from_source(source=issue_events_source, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="issue_events_source")\
            .map(map_event_string_to_event_dict)
            
        
# endregion

# IV. Transform datastreams, extract fields and store into Cassandra tables
#region 

max_concurrent_requests = 1000
cassandra_host = 'cassandra_stelios'
cassandra_port = 9142
print(f"Start reading data from kafka topics to create "
        f"Cassandra tables:\n"
        "T11_12: pull_request_closing_times, T13_14: issue_closing_times\n"
        "T15: issue_closing_times_by_label\n")



# Q11_12: Closing times of pull requests
# region

def keep_closed_pull_requests(event_dict):
    if event_dict["action"] == "closed":
        return True
    else:
        return False

def create_row_q11_12(event_dict):
    return Row(event_dict["opening_time"], 
               event_dict["closing_time"], 
               event_dict["repo"],
               event_dict["pull_request_number"])


pull_request_closing_times_type_info_q11_12 = \
    Types.ROW_NAMED(['opening_time', 'closing_time', 'repo_name', 'pull_request_number'], \
    [Types.STRING(), Types.STRING(), \
        Types.STRING(),  Types.INT()])
pull_request_closing_times_ds_q11_12 = pull_request_events_ds\
    .filter(keep_closed_pull_requests)\
    .map(create_row_q11_12, output_type=pull_request_closing_times_type_info_q11_12) 


upsert_element_into_pull_request_closing_times_q11_12 = \
            "UPDATE prod_gharchive.pull_request_closing_times "\
            "SET opening_time = ?, closing_time = ? WHERE "\
            "repo_name = ? and pull_request_number = ?;"
cassandra_sink_q11_12 = CassandraSink.add_sink(pull_request_closing_times_ds_q11_12)\
    .set_query(upsert_element_into_pull_request_closing_times_q11_12)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()
# endregion



# Q13_14: Issue closing times
# region
def keep_closed_issues(event_dict):
    if event_dict["payload"]["action"] == "closed":
        return True
    else:
        return False

def create_row_q13_14(event_dict):
    return Row(event_dict["opening_time"],
               event_dict["closing_time"],
               event_dict["repo"],
               event_dict["issue_number"])

issue_closing_times_type_info_q13_14 = \
    Types.ROW_NAMED(['opening_time', 'closing_time', 'repo_name', \
        'issue_number'], \
    [Types.STRING(), Types.STRING(), \
        Types.STRING(),  Types.INT()])
issue_closing_times_ds_q13_14 = issue_events_ds\
    .filter(keep_closed_issues)\
    .map(create_row_q13_14, output_type=issue_closing_times_type_info_q13_14)

upsert_element_into_issue_closing_times_q13_14 = \
            "UPDATE prod_gharchive.issue_closing_times "\
            "SET opening_time = ?, closing_time = ? WHERE "\
            "repo_name = ? and issue_number = ?;"
cassandra_sink_q13_14 = CassandraSink.add_sink(issue_closing_times_ds_q13_14)\
    .set_query(upsert_element_into_issue_closing_times_q13_14)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()
# endregion



# Q15: Issue closing times by label
# region

def create_row_q15(event_dict):
    return Row(event_dict["opening_time"],
               event_dict["closing_time"],
               event_dict["repo"],
               event_dict["labels"],
               event_dict["issue_number"])
    
def split_issue_labels(closed_issue_row_with_list_of_labels):
    
    opening_time = closed_issue_row_with_list_of_labels[0]
    closing_time = closed_issue_row_with_list_of_labels[1]
    repo_name = closed_issue_row_with_list_of_labels[2]
    labels_list = closed_issue_row_with_list_of_labels[3]
    issue_number = closed_issue_row_with_list_of_labels[4]
    
    # If there are no labels, emit a single issue event element with label 
    if len(labels_list) == 0:
        label = ""
        return Row(opening_time, closing_time, repo_name, label, issue_number)
    # else, create a list of rows with the separated labels 
    # (one label per issue row) and emit a single label element one at a time
    else:
        issue_row_list = []
        for single_label in labels_list:
            single_label_issue_row = \
                Row(opening_time, closing_time, repo_name, single_label, issue_number)
            issue_row_list.append(single_label_issue_row)
        yield from issue_row_list


issue_closing_times_type_info_with_list_of_labels_q15 = \
    Types.ROW_NAMED(['opening_time', 'closing_time', 'repo_name', \
        'labels', 'issue_number'], \
    [Types.STRING(), Types.STRING(), \
        Types.STRING(), Types.LIST(Types.STRING()),  Types.INT()])  
issue_closing_times_type_info_with_single_label = \
    Types.ROW_NAMED(['opening_time', 'closing_time', 'repo_name', \
        'label', 'issue_number'], \
    [Types.STRING(), Types.STRING(), \
        Types.STRING(), Types.STRING(),  Types.INT()])
issue_closing_times_by_label_ds_q15 = issue_events_ds\
    .filter(keep_closed_issues)\
    .map(create_row_q15, \
            output_type=issue_closing_times_type_info_with_list_of_labels_q15)\
    .flat_map(split_issue_labels, output_type=issue_closing_times_type_info_with_single_label)


insert_element_into_issue_closing_times_by_label_q15 = \
    "INSERT INTO prod_gharchive.issue_closing_times_by_label "\
    "(opening_time, closing_time, repo_name, label, issue_number) "\
    "VALUES (?, ?, ?, ?, ?);"
cassandra_sink_q15 = CassandraSink.add_sink(issue_closing_times_by_label_ds_q15)\
    .set_query(insert_element_into_issue_closing_times_by_label_q15)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()

# endregion

# endregion


if __name__ == "__main__":
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

    # Screen 4
    create_pull_request_closing_times_table_q11_12 = \
        "CREATE TABLE IF NOT EXISTS prod_gharchive.pull_request_closing_times "\
        "(repo_name text, pull_request_number int, opening_time text, "\
        "closing_time text, PRIMARY KEY ((repo_name), "\
        "pull_request_number)) WITH CLUSTERING ORDER BY "\
        "(pull_request_number ASC);"
    session.execute(create_pull_request_closing_times_table_q11_12)

    create_issue_closing_times_table_q13_14 = \
        "CREATE TABLE IF NOT EXISTS prod_gharchive.issue_closing_times "\
        "(repo_name text, issue_number int, opening_time text, "\
        "closing_time text, PRIMARY KEY ((repo_name), "\
        "issue_number)) WITH CLUSTERING ORDER BY "\
        "(issue_number ASC);"
    session.execute(create_issue_closing_times_table_q13_14)

    create_issue_closing_times_by_label_table_q15 = \
        "CREATE TABLE IF NOT EXISTS prod_gharchive.issue_closing_times_by_label "\
        "(repo_name text, issue_number int, opening_time text, "\
        "closing_time text, label text, PRIMARY KEY ((repo_name), "\
        "label, issue_number)) WITH CLUSTERING ORDER BY "\
        "(label ASC, issue_number ASC);"
    session.execute(create_issue_closing_times_by_label_table_q15)
    
        
    cluster.shutdown()
    
    # Execute the flink streaming environment
    env.execute(os.path.splitext(os.path.basename(__file__))[0])

