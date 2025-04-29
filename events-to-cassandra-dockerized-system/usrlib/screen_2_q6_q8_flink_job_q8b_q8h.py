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

from pyflink.datastream.connectors.kafka import KafkaSource, \
    KafkaOffsetResetStrategy, KafkaOffsetsInitializer
from pyflink.datastream.connectors.cassandra import CassandraSink


from argparse import ArgumentParser, RawDescriptionHelpFormatter
from configparser import ConfigParser

from cassandra.cluster import Cluster 
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

# III. Consume datastreams
# region
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


# Consume all events
screen_2_all_events_consumer_group_id = "screen_2_all_events_consumer_group_id"
all_events_topic = "all_events"    
all_events_source = create_kafka_source(kafka_bootstrap_servers=kafka_bootstrap_servers,
                                         consumer_group_id=screen_2_all_events_consumer_group_id,
                                         topic=all_events_topic,
                                         kafka_properties=kafka_props)
all_events_ds = env.from_source(source=all_events_source, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="all_events_source")\
            .map(map_event_string_to_event_dict)
            
            


# IV. Transform datastreams, extract fields and store into Cassandra tables
#region 
max_concurrent_requests = 1000
cassandra_host = 'cassandra_stelios'
cassandra_port = 9142
cassandra_keyspace = "prod_gharchive"
print(f"Start reading data from kafka topics to create "
        f"Cassandra tables:\n"
        "T8_b: number_of_events_by_bots, T8_h: number_of_events_by_humans")


# Q8_b: Number of events by bots
# region
def keep_bot_events(event_dict):
    if event_dict["username"].endswith('[bot]'):
        return True
    else:
        return False
    
def keep_human_events(event_dict):
    if not event_dict["username"].endswith('[bot]'):
        return True
    else:
        return False
    
def create_row_8(event_dict):
    number_of_events = 1
    return Row(number_of_events,
               event_dict["type"], 
               event_dict["day"])

number_of_bot_events_per_type_by_day_type_info_q8_b = \
    Types.ROW_NAMED(['number_of_events', 'event_type', 'day'], \
    [Types.LONG(), Types.STRING(), \
        Types.STRING()])
number_of_events_info_ds_q8_b = all_events_ds\
        .filter(keep_bot_events) \
        .map(create_row_8, 
                output_type=number_of_bot_events_per_type_by_day_type_info_q8_b)

upsert_element_into_number_of_bot_events_per_type_by_day_q8_b = \
            "UPDATE {0}.number_of_bot_events_per_type_by_day "\
            "SET number_of_events = number_of_events + ? WHERE "\
            "event_type = ? AND day = ?;".format(cassandra_keyspace)
cassandra_sink_q8_b = CassandraSink.add_sink(number_of_events_info_ds_q8_b)\
    .set_query(upsert_element_into_number_of_bot_events_per_type_by_day_q8_b)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()

#endregion



# Q8_h: Number of events by humans
# region
number_of_human_events_per_type_by_day_type_info_q8_h = \
    number_of_bot_events_per_type_by_day_type_info_q8_b
number_of_events_info_ds_q8_h = all_events_ds\
            .filter(keep_human_events) \
            .map(create_row_8, \
                    output_type=number_of_human_events_per_type_by_day_type_info_q8_h)\
                    
upsert_element_into_number_of_human_events_per_type_by_day_q8_h = \
            "UPDATE {0}.number_of_human_events_per_type_by_day "\
            "SET number_of_events = number_of_events + ? WHERE "\
            "event_type = ? AND day = ?;".format(cassandra_keyspace)
cassandra_sink_q8_h = CassandraSink.add_sink(number_of_events_info_ds_q8_h)\
    .set_query(upsert_element_into_number_of_human_events_per_type_by_day_q8_h)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()

# endregion

# endregion


if __name__ =='__main__':
    
    # Create cassandra keyspace if not exist
    cluster = Cluster([cassandra_host],port=cassandra_port, connect_timeout=10)
    # Connect without creating keyspace. Once connected create the keyspace
    session = cluster.connect()
    create_keyspace = "CREATE KEYSPACE IF NOT EXISTS "\
        "{0} WITH replication = {'class': 'SimpleStrategy', "\
        "'replication_factor': '1'} AND durable_writes = true;".format(cassandra_keyspace)
    session.execute(create_keyspace)

    session = cluster.connect(cassandra_keyspace, wait_for_all_pools=True)
    session.execute(f'USE {cassandra_keyspace}')


    # Screen 2
    create_number_of_events_by_bots_q8_b = \
        "CREATE TABLE IF NOT EXISTS {0}.number_of_bot_events_per_type_by_day "\
        "(day text, event_type text, number_of_events counter, PRIMARY KEY ((day), "\
        "event_type)) WITH CLUSTERING ORDER BY "\
        "(event_type ASC);".format(cassandra_keyspace)
    session.execute(create_number_of_events_by_bots_q8_b)


    create_number_of_all_events_by_humans_q8_h = \
        "CREATE TABLE IF NOT EXISTS {0}.number_of_human_events_per_type_by_day "\
        "(day text, event_type text, number_of_events counter, PRIMARY KEY ((day), "\
        "event_type)) WITH CLUSTERING ORDER BY "\
        "(event_type ASC);".format(cassandra_keyspace)
    session.execute(create_number_of_all_events_by_humans_q8_h)
        
    cluster.shutdown()
    
    # Execute the flink streaming environment
    env.execute(os.path.splitext(os.path.basename(__file__))[0])

