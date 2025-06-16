
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
# region
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.set_parallelism(1)
env.add_jars("file:///opt/flink/opt/flink-sql-connector-kafka-3.0.2-1.18.jar")
env.add_jars("file:///opt/flink/opt/flink-connector-cassandra_2.12-3.2.0-1.18.jar")
env.set_restart_strategy(RestartStrategies.\
    fixed_delay_restart(restart_attempts=3, delay_between_attempts=1000))
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
# endregion



# Common configs for all datastream transformations
near_real_time_events_topic = "near-real-time-raw-events"
kafka_props = {'enable.auto.commit': 'true',
               'auto.commit.interval.ms': '1000',
               'auto.offset.reset': 'smallest'}
def map_event_string_to_event_dict(event_string):
    return eval(event_string)
cassandra_host = 'cassandra_stelios'
cassandra_port = 9142
cassandra_keyspace = "near_real_time_data"
max_concurrent_requests = 1000



# Q1. stats_by_day
# region
stats_consumer_group_id = 'raw_events_to_stats_by_day_consumer_group'
kafka_consumer_stats = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .set_group_id(stats_consumer_group_id)\
            .set_topics(near_real_time_events_topic) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_props)\
            .build()

            

raw_events_to_stats_ds = env.from_source( source=kafka_consumer_stats, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="kafka_source")\
            .map(map_event_string_to_event_dict)\
                # .set_parallelism(16)
print(f"Started reading data from kafka topic '{near_real_time_events_topic}' to create "
        "topic 'stats_by_day'")




def filter_no_statistics_events(eventDict):
    
    event_type = eventDict["type"]
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
    

def extract_statistics_and_create_row(eventDict):

    # (Example) From 2024-01-01T00:00:01Z get 2024-01-01  
    day = eventDict["created_at"].split('T', 1)[0]
    # Initialize the statistics per day 
    # for the cassandra update counter columns query
    commits = 0
    stars = 0
    forks = 0
    pull_requests = 0
    
    event_type = eventDict["type"]                        
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

stats_type_info = Types.ROW_NAMED(['commits', 'stars', 'forks', \
            'pull_requests', 'day'], [Types.LONG(), Types.LONG(),  \
            Types.LONG(), Types.LONG(), Types.STRING()])
   



stats_ds = raw_events_to_stats_ds.filter(filter_no_statistics_events)\
                        .map(extract_statistics_and_create_row, \
                           output_type=stats_type_info)
stats_table = 'stats_by_day'
upsert_element_into_stats_by_day_q1 = \
            f"UPDATE {cassandra_keyspace}.{stats_table} \
                    SET commits = commits + ?, stars = stars + ?, \
                    forks = forks + ?, pull_requests = pull_requests + ? WHERE \
                    day = ?;"

print(f"Started inserting data from kafka topics into Cassandra tables:\n"
        "T1: stats_by_day, T2_3: most_popular_repos_by_day,\n"
        "T4: most_popular_languages_by_day, T5: most_popular_topics_by_day")
cassandra_sink_q1 = CassandraSink.add_sink(stats_ds)\
    .set_query(upsert_element_into_stats_by_day_q1)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()

# # Print transformed datastream
# stats_ds.print()

# endregion



















# Q2_3. most_popular_repos_by_day
# region
most_popular_repos_consumer_group_id = 'raw_events_to_most_popular_repos_by_day_consumer_group'
kafka_consumer_most_popular_repos = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .set_group_id(most_popular_repos_consumer_group_id)\
            .set_topics(near_real_time_events_topic) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_props)\
            .build()

raw_events_to_most_pop_repos_ds = env.from_source( 
            source=kafka_consumer_most_popular_repos, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="kafka_source")\
            .map(map_event_string_to_event_dict)\
                # .set_parallelism(16)
print(f"Started reading data from kafka topic '{near_real_time_events_topic}' to create "
        "topic 'most_popular_repos_by_day'")




def filter_non_watch_and_fork_events(eventDict):
    event_type = eventDict["type"]
    if event_type == "WatchEvent":
        return True
    elif event_type == "ForkEvent":
        return True
    # If none of the above works, return False (filter the event out)
    return False
    

def extract_repo_stars_forks_and_create_row(eventDict):

    # (Example) From 2024-01-01T00:00:01Z get 2024-01-01  
    day = eventDict["created_at"].split('T', 1)[0]
    repo = eventDict["repo"]["full_name"]
    stars = 0
    forks = 0
    event_type = eventDict["type"]                        
    if event_type == "WatchEvent":
        stars = 1
    elif event_type == "ForkEvent":
        forks = 1
    else: 
        raise ValueError(f"Event with type '{event_type}' was not filtered")
    return Row(stars, forks, day, repo)


most_popular_repos_type_info = Types.ROW_NAMED(['stars', 'forks', \
            'day', 'repo'], [Types.LONG(), Types.LONG(),  \
            Types.STRING(), Types.STRING()])
   



most_popular_repos_ds = raw_events_to_most_pop_repos_ds\
        .filter(filter_non_watch_and_fork_events)\
        .map(extract_repo_stars_forks_and_create_row, \
            output_type=most_popular_repos_type_info)
        
most_popular_repos_table = 'most_popular_repos_by_day'
upsert_element_into_most_popular_repos_by_day_q2_3 = \
            f"UPDATE {cassandra_keyspace}.{most_popular_repos_table} \
                    SET stars = stars + ?, forks = forks + ? \
                    WHERE day = ? AND repo = ?;"

print(f"Started inserting data from {near_real_time_events_topic} into Cassandra table:\n"
        f"T2_3: '{most_popular_repos_table}'")
cassandra_sink_q2_3 = CassandraSink.add_sink(most_popular_repos_ds)\
    .set_query(upsert_element_into_most_popular_repos_by_day_q2_3)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()

# # Print transformed datastream
# most_popular_repos_ds.print()

# endregion




# Q4. most_popular_languages_by_day
# region
langs_consumer_group_id = 'raw_events_to_langs_by_day_consumer_group'
kafka_consumer_langs = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .set_group_id(langs_consumer_group_id)\
            .set_topics(near_real_time_events_topic) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_props)\
            .build()

            

raw_events_to_langs_ds = env.from_source( source=kafka_consumer_langs, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="kafka_source")\
            .map(map_event_string_to_event_dict)\
                # .set_parallelism(16)
print(f"Started reading data from kafka topic '{near_real_time_events_topic}' to create "
        "topic 'most_popular_languages_by_day'")


def filter_no_languages_events(eventDict):
    event_types_with_languages = ["PullRequestEvent", \
    "PullRequestReviewEvent", "PullRequestReviewCommentEvent", \
    "PullRequestReviewThreadEvent"]

    event_type = eventDict["type"]    
    # Filter out events without languages (non Pull request events)
    if event_type not in event_types_with_languages: 
        return False
    else:
        language = str(eventDict["payload"]["pull_request"]["base"]["repo"]["language"])
        # Filter out pull request events without a language declaration
        if language == 'None':
            return False
        return True

def extract_language_info_and_create_row(eventDict):
    day = eventDict["created_at"].split('T', 1)[0]
    num_of_occurrences = 1
    language = str(eventDict["payload"]["pull_request"]["base"]["repo"]["language"])
    return Row(num_of_occurrences, day, language)
    
    
popular_langs_type_info = Types.ROW_NAMED(['num_of_occurrences', 'day', 'language'], \
            [Types.LONG(), Types.STRING(), Types.STRING()])
   


langs_ds = raw_events_to_langs_ds.filter(filter_no_languages_events)\
                        .map(extract_language_info_and_create_row, \
                           output_type=popular_langs_type_info)
most_popular_languages_table = 'most_popular_languages_by_day'
upsert_element_into_pop_langs_by_day_q4 = \
            f"UPDATE {cassandra_keyspace}.{most_popular_languages_table} \
                    SET num_of_occurrences = num_of_occurrences + ? WHERE \
                    day = ? AND language = ?;"

print(f"Started inserting data from kafka topic {near_real_time_events_topic} into Cassandra table:\n"
        "T4: most_popular_languages_by_day")
cassandra_sink_q4 = CassandraSink.add_sink(langs_ds)\
    .set_query(upsert_element_into_pop_langs_by_day_q4)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()

# # Print transformed datastream
# stats_ds.print()

# endregion





# Q5. most_popular_topics_by_day
# region
topics_consumer_group_id = 'raw_events_to_topics_by_day_consumer_group'
kafka_consumer_topics = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .set_group_id(topics_consumer_group_id)\
            .set_topics(near_real_time_events_topic) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_props)\
            .build()

            

raw_events_to_topics_ds = env.from_source( source=kafka_consumer_topics, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="kafka_source")\
            .map(map_event_string_to_event_dict)\
                # .set_parallelism(16)
print(f"Started reading data from kafka topic '{near_real_time_events_topic}' to create "
        "topic 'most_popular_topics_by_day'")


def filter_no_topics_events(eventDict):
    event_types_with_topics = ["PullRequestEvent", \
    "PullRequestReviewEvent", "PullRequestReviewCommentEvent", \
    "PullRequestReviewThreadEvent"]

    event_type = eventDict["type"]    
    # Filter out events without topics (non Pull request events)
    if event_type not in event_types_with_topics: 
        return False
    else:
        topics = str(eventDict["payload"]["pull_request"]["base"]["repo"]["topics"])
        # Filter out pull request events without a topic declaration
        if topics == 'None':
            return False
        return True


def extract_topic_info_and_create_row(eventDict):
    day = eventDict["created_at"].split('T', 1)[0]
    num_of_occurrences = 1
    topics = eventDict["payload"]["pull_request"]["base"]["repo"]["topics"]
    for topic in topics: 
        yield Row(num_of_occurrences, day, topic)
    
    
popular_topics_type_info = Types.ROW_NAMED(['num_of_occurrences', 'day', 'topic'], \
            [Types.LONG(), Types.STRING(), Types.STRING()])
   


topics_ds = raw_events_to_topics_ds.filter(filter_no_topics_events)\
                        .flat_map(extract_topic_info_and_create_row, \
                           output_type=popular_topics_type_info)
most_popular_topics_table = 'most_popular_topics_by_day'
upsert_element_into_pop_topics_by_day_q5 = \
            f"UPDATE {cassandra_keyspace}.{most_popular_topics_table} \
                    SET num_of_occurrences = num_of_occurrences + ? WHERE \
                    day = ? AND topic = ?;"

print(f"Started inserting data from kafka topic {near_real_time_events_topic} into Cassandra table:\n"
        "T5: 'most_popular_topics_by_day'")
cassandra_sink_q5 = CassandraSink.add_sink(topics_ds)\
    .set_query(upsert_element_into_pop_topics_by_day_q5)\
    .set_host(host=cassandra_host, port=cassandra_port)\
    .set_max_concurrent_requests(max_concurrent_requests)\
    .enable_ignore_null_fields()\
    .build()

# # Print transformed datastream
# topics_ds.print()

# endregion






# real time stars forks 


if __name__ =='__main__':
    
    # Create cassandra keyspace if not exist
    cluster = Cluster([cassandra_host], port=cassandra_port, connect_timeout=10)
    session = cluster.connect()
    create_keyspace = f"CREATE KEYSPACE IF NOT EXISTS \
        {cassandra_keyspace} WITH replication = \
        {{'class': 'SimpleStrategy', 'replication_factor': '1'}}\
        AND durable_writes = true;"
    session.execute(create_keyspace)
    session = cluster.connect(cassandra_keyspace, wait_for_all_pools=True)
    session.execute(f'USE {cassandra_keyspace}')

    
    # Screen 1
    # Statistics
    # Q1. stats_by_day
    stats_table = 'stats_by_day'
    create_stats_table =  \
                f"CREATE TABLE IF NOT EXISTS {cassandra_keyspace}.{stats_table} \
                (day text, commits counter, stars counter, pull_requests counter, \
                forks counter, PRIMARY KEY (day));"
    session.execute(create_stats_table)

    # Trending repos table
    # Q2_3. most_popular_repos_by_day
    most_popular_repos_table = "most_popular_repos_by_day"
    create_most_popular_repos_table = \
            f"CREATE TABLE IF NOT EXISTS {cassandra_keyspace}.{most_popular_repos_table} \
            (day text, repo text, stars counter, forks counter, PRIMARY KEY ((day), repo)) WITH CLUSTERING ORDER BY (repo desc);"
    session.execute(create_most_popular_repos_table)
    
    # Trending languages table
    # Q4. most_popular_languages_by_day
    most_popular_languages_table = "most_popular_languages_by_day"
    create_pop_langs_table = \
            f"CREATE TABLE IF NOT EXISTS {cassandra_keyspace}.{most_popular_languages_table} \
            (day text, language text, num_of_occurrences counter, PRIMARY KEY ((day), language)) WITH CLUSTERING ORDER BY (language desc);"
    session.execute(create_pop_langs_table)

    # Trending topics table 
    # Q5. most_popular_topics_by_day
    popular_topics_table = "most_popular_topics_by_day"
    create_pop_topics_table = \
            f"CREATE TABLE IF NOT EXISTS {cassandra_keyspace}.{popular_topics_table} \
            (day text, topic text, num_of_occurrences counter, PRIMARY KEY ((day), topic)) WITH CLUSTERING ORDER BY (topic desc);"
    session.execute(create_pop_topics_table)

    # Note on "Number of events per second sliding bar graph":
    # Near real time data per second are created from the producer producing into the one partitioned topic 'near-real-time-raw-events-ordered' which maintains its order by using a single partition  
    
    # Note on "Near real time stars and forks":
    # No table to make for the near real time stars and forks section 
    # (all is done with the datastream transformation and reenter into kafka topic 
    # near-real-time-stars-forks)
    

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