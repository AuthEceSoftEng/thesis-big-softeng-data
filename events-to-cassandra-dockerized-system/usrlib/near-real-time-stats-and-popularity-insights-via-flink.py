'''
Creates three datastreams from raw-events to monitor the total number of 
commits, stars, forks and pull-requests. Also, datastreams regarding the popularity 
of programming languages and topics appearing in github events

More specifically:
Creates datastreams:
D1_2_3: Sums up the total number of commits, stars, forks, pull-requests on a particular day.
Has elements: (day, repo_name, stars, forks, commits, pull_requests)

D4: Stores repo languages and its number of occurences on events. 
Has elements: (day, language, number_of_events)

D5: Stores popular topics every day.
Has elements: (day, topic, number_of_events)
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


########################################################################################
# D1_2_3: stats_by_day (day text, repo_name text, commits int, open_issues int, 
# closed_issues int, stars int, forks int, pull-requests int) 
# Key to key by: (day, repo_name)
########################################################################################
# region





# Type info for statistics
stats_type_info = Types.ROW_NAMED(['day', 'repo_name', 'commits', 'open_issues', \
                            'closed_issues', 'stars', 'forks', 'pull_requests'], \
                    [Types.STRING(), Types.STRING(), Types.INT(), Types.INT(), \
                        Types.INT(), Types.INT(), Types.INT(), Types.INT()])


record_schema = JsonRowSerializationSchema.builder() \
                    .with_type_info(stats_type_info) \
                    .build()

topic_to_produce_stats_into = 'stats_by_day'

stats_record_serializer = KafkaRecordSerializationSchema.builder() \
    .set_topic(topic_to_produce_stats_into) \
    .set_value_serialization_schema(record_schema) \
    .build()

# kafka_bootstrap_servers = "kafka:9092"

kafka_stats_producer = KafkaSink.builder() \
    .set_bootstrap_servers(kafka_bootstrap_servers) \
    .set_record_serializer(stats_record_serializer) \
    .build()

kafka_props = {'enable.auto.commit': 'true',
               'auto.commit.interval.ms': '1000',
               'auto.offset.reset': 'smallest'}


# Kafka consumer from kafka topic "near-real-time-raw-events"
topic_to_consume_from = "near-real-time-raw-events"
kafka_consumer_stats = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .set_group_id('raw_events_to_stats_by_day_consumer_group')\
            .set_topics(topic_to_consume_from) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_props)\
            .build()

            
# Consume original datastream
print("start reading data from kafka topic 'near-real-time-raw-events' to create \n"
        "topic 'stats_by_day'")
raw_events_ds = env.from_source( source=kafka_consumer_stats, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="kafka_source")\
                # .set_parallelism(16)


# Event types where the needed fields reside:
event_types_with_info = ["PushEvent", \
    "IssuesEvent", "WatchEvent", "ForkEvent", "PullRequestEvent"]


# Filter out events of type that contain no info we need
def filter_no_statistics_events(eventString):
    global event_types_with_info 
    eventDict = eval(eventString)
    event_type = eventDict["type"]
    
    # Acceleration attempt:
    if event_type == "PushEvent":
        return True
    elif event_type == "IssuesEvent":
        # Filter out IssueEvents that do not open or close issues
        if eventDict["payload"]["action"] == "opened" or \
            eventDict["payload"]["action"] == "closed":
            return True
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

    
# Function for the original raw-events datastream
def extract_statistics_and_create_row(eventString):
    eventDict = eval(eventString)
    # eventDict = json.loads(eventString)
    event_type = eventDict["type"]
    
    
    # Removing trailing characters after T that gives us the time
    # leaves us with the day 
    # Split 2024-01-01T00:00:01Z into 2024-01-01 and T00:00:01Z 
    # and keep only the first one
    day = eventDict["created_at"].split('T', 1)[0]
    repo_name = eventDict["repo"]["full_name"]
    # Initialize the statistics per repo and day
    commits = 0
    open_issues = 0
    closed_issues = 0
    stars = 0
    forks = 0
    pull_requests = 0
                            
    if event_type == "PushEvent":
        commits = 0
        # Keep regular events
        if (commits <= 100) or (commits <= 200
        and eventDict["payload"]["size"] == eventDict["payload"]["distinct_size"]):
            commits = eventDict["payload"]["distinct_size"]
    elif event_type == "IssuesEvent" and \
        eventDict["payload"]["action"] == "opened":
        open_issues = 1
    elif event_type == "IssuesEvent" and \
        eventDict["payload"]["action"] == "closed":
         closed_issues = 1
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
         
         
    repo_row = Row(day, repo_name, commits, open_issues, \
        closed_issues, stars, forks, pull_requests)
    return repo_row

# Aggregate statistics by (day, repo_name)
stats_type_info_to_key_by = Types.ROW_NAMED(['day', 'repo_name'],\
        [Types.STRING(), Types.STRING()])


def key_stats_row(eventRow):
    day = eventRow[0]
    repo_name =  eventRow[1]
    return Row(day, repo_name)

    
def aggregate_day_stats(row_keyed_i, row_keyed_j):
    # Day and repo_name remain the same
    new_repo_name = row_keyed_i[0]
    new_day = row_keyed_i[1]
    # Statistics to aggregate
    new_commits = row_keyed_i[2] + row_keyed_j[2]
    new_open_issues = row_keyed_i[3] + row_keyed_j[3]
    new_closed_issues = row_keyed_i[4] + row_keyed_j[4]
    new_stars = row_keyed_i[5] + row_keyed_j[5]
    new_forks = row_keyed_i[6] + row_keyed_j[6]
    new_pull_requests = row_keyed_i[7] + row_keyed_j[7]
    
    return Row(new_day, new_repo_name, new_commits, new_open_issues,\
        new_closed_issues, new_stars, new_forks, new_pull_requests)

# Transform the raw-events datastream into a datastream of repos' stats:
# {day: <string>, repo_name: <string>, commits: <int>, 
# open_issues: <int>, closed_issues: <int>, stars: <int>, 
# forks: <int>, pull_requests <int>}

stats_ds = raw_events_ds.filter(filter_no_statistics_events)\
                        .map(extract_statistics_and_create_row, \
                           output_type=stats_type_info)\
                        .key_by(key_stats_row, \
                            key_type=stats_type_info_to_key_by)\
                        .reduce(aggregate_day_stats)\
                        .set_parallelism(4)

# Produce transformed datastream
stats_ds.sink_to(kafka_stats_producer)\
    # .set_parallelism(16)

# # Print transformed datastream
# stats_ds.print()

# endregion

########################################################################################
# D4: popular_languages_by_day (day text, language text,  number_of_events int) 
# Key to key by: (day, language)
########################################################################################
# region


# Event types where language and topics fields reside
event_types_with_languages = ["PullRequestEvent", \
    "PullRequestReviewEvent", "PullRequestReviewCommentEvent"]


# Producer to kafka topic "repos"
language_per_day_type_info = Types.ROW_NAMED(['day', 'language', 'number_of_events'],\
    [Types.STRING(), Types.STRING(), Types.INT()])


record_schema = JsonRowSerializationSchema.builder() \
                    .with_type_info(language_per_day_type_info) \
                    .build()

topic_to_produce_into = 'popular_languages_by_day'

language_record_serializer = KafkaRecordSerializationSchema.builder() \
    .set_topic(topic_to_produce_into) \
    .set_value_serialization_schema(record_schema) \
    .build()

# kafka_bootstrap_servers = "kafka:9092"

kafka_languages_producer = KafkaSink.builder() \
    .set_bootstrap_servers(kafka_bootstrap_servers) \
    .set_record_serializer(language_record_serializer) \
    .build()


    
# Kafka consumer from kafka topic "near-real-time-raw-events"
topic_to_consume_from = "near-real-time-raw-events"
kafka_props = {'enable.auto.commit': 'true',
               'auto.commit.interval.ms': '1000',
               'auto.offset.reset': 'smallest'}

# .set_starting_offsets(KafkaOffsetsInitializer.\
#                 committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
                

            
                    
kafka_consumer_repos = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_group_id('raw_events_to_language_popularity_by_day_consumer_group') \
            .set_topics(topic_to_consume_from) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_props)\
            .set_starting_offsets(KafkaOffsetsInitializer.\
                committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .build()

# Consume original datastream
print("start reading data from kafka topic 'near-real-time-raw-events' to create \n"
      "topic 'popular_languages_by_day'")
raw_events_ds = env.from_source( source=kafka_consumer_repos, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="kafka_source")

def filter_no_language_events(eventString):
    global event_types_with_languages 
    eventDict = eval(eventString)
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

# Function for the original raw-events datastream
def extract_language_info_and_create_row(eventString):
    eventDict = eval(eventString)
    day = eventDict["created_at"].split('T', 1)[0]
    number_of_events = 1
    language = str(eventDict["payload"]["pull_request"]["base"]["repo"]["language"])
    repo_row = Row(day, language, number_of_events)
    return repo_row


day_and_language_to_key_by = Types.ROW_NAMED(['day', 'language'],\
        [Types.STRING(), Types.STRING()])

def key_language_row(eventRow):
    try: 
        day = eventRow[0]
        language =  eventRow[1]
        return Row(day, language)
    except Exception:
        print(f"Event on exception: {eventRow}")


def aggregate_language_number_of_events(row_keyed_i, row_keyed_j):
    # Day and language remain the same
    new_day = row_keyed_i[0]
    new_language = row_keyed_i[1]
    
    # Aggregate number of occurrences of the language in the events
    new_number_of_events = row_keyed_i[2] + row_keyed_j[2]
    
    return Row(new_day, new_language, new_number_of_events)


# Transform the near-real-time-raw-events datastream into 
# a datastream of languages per day' info:
# {day: <string>, language: <string>, number_of_events: <string>}
language_per_day_ds = raw_events_ds.filter(filter_no_language_events) \
                        .map(extract_language_info_and_create_row, \
                           output_type=language_per_day_type_info) \
                        .key_by(key_language_row, key_type=day_and_language_to_key_by) \
                        .reduce(aggregate_language_number_of_events)

# Produce transformed datastream
language_per_day_ds.sink_to(kafka_languages_producer)

# # Print transformed datastream
# language_per_day_ds.print()


# endregion


########################################################################################
# D5: popular_topics_by_day (day text,  number_of_events int, topic text)
# Key to key by: (day, topic)
########################################################################################
# region


# Event types where language and topics fields reside
event_types_with_topics = ["PullRequestEvent", \
    "PullRequestReviewEvent", "PullRequestReviewCommentEvent"]


# Producer to kafka topic "popular_topics_by_day"
topic_per_day_type_info = Types.ROW_NAMED(['day', 'topic', 'number_of_events'],\
    [Types.STRING(), Types.STRING(), Types.INT()])


record_schema = JsonRowSerializationSchema.builder() \
                    .with_type_info(topic_per_day_type_info) \
                    .build()

topic_to_produce_topics_into = 'popular_topics_by_day'

topic_record_serializer = KafkaRecordSerializationSchema.builder() \
    .set_topic(topic_to_produce_topics_into) \
    .set_value_serialization_schema(record_schema) \
    .build()

# kafka_bootstrap_servers = "kafka:9092"

kafka_topics_producer = KafkaSink.builder() \
    .set_bootstrap_servers(kafka_bootstrap_servers) \
    .set_record_serializer(topic_record_serializer) \
    .build()

                  

    
# Kafka consumer from kafka topic "near-real-time-raw-events"
topic_to_consume_from = "near-real-time-raw-events"
kafka_props = {'enable.auto.commit': 'true',
               'auto.commit.interval.ms': '1000',
               'auto.offset.reset': 'smallest'}

# .set_starting_offsets(KafkaOffsetsInitializer.\
#                 committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
                

            
                    
kafka_consumer_topics = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap_servers) \
            .set_group_id('raw_events_to_topic_popularity_by_day_consumer_group') \
            .set_topics(topic_to_consume_from) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties(kafka_props)\
            .set_starting_offsets(KafkaOffsetsInitializer.\
                committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
            .build()

# Consume original datastream
print("start reading data from kafka topic 'near-real-time-raw-events' to create \n"
            "topic 'popular_topics_by_day'")
topics_ds = env.from_source( source=kafka_consumer_topics, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="kafka_source")


def filter_no_topic_events(eventString):
    global event_types_with_topics
    eventDict = eval(eventString)
    event_type = eventDict["type"]
    
    if event_type in event_types_with_topics: 
        # Keeps pull request related events
        return True
    else:
        return False



# Function for the original raw-events datastream
def extract_topic_info_and_create_row(eventString):

    eventDict = eval(eventString)
    
    # Removing trailing characters after T that gives us the time
    # leaves us with the day 
    # Split 2024-01-01T00:00:01Z into 2024-01-01 and T00:00:01Z 
    # and keep only the first one
    day = eventDict["created_at"].split('T', 1)[0]
    number_of_events = 1
    
    # Returns a list with the topics of the repo
    topics = eventDict["payload"]["pull_request"]["base"]["repo"]["topics"]
    
    for topic in topics:
        yield Row(day, topic, number_of_events)




topic_and_day_to_key_by = Types.ROW_NAMED(['day', 'topic'],\
    [Types.STRING(), Types.STRING()])

def key_topic_row(eventRow):
    # eventRowList = list(eventRow)
    day = eventRow[0]
    topic =  eventRow[1]
    return Row(day, topic)


def aggregate_topic_number_of_events(row_keyed_i, row_keyed_j):
    # Day of topic remain the same
    new_day = row_keyed_i[0]
    new_topic = row_keyed_i[1]
    
    # Aggregate number of occurrences of the topics in the events
    new_number_of_events = row_keyed_i[2] + row_keyed_j[2]
    
    return Row(new_day, new_topic, new_number_of_events)


# Transform the raw-events datastream into a datastream of repos' info:
# {repo_name: <string>, language: <string>, topics: <string>}
topics_by_day_ds = topics_ds.filter(filter_no_topic_events) \
                        .flat_map(extract_topic_info_and_create_row, \
                           output_type=topic_per_day_type_info) \
                        .key_by(key_topic_row, key_type=topic_and_day_to_key_by) \
                        .reduce(aggregate_topic_number_of_events)

# Produce transformed datastream
topics_by_day_ds.sink_to(kafka_topics_producer)

# Print transformed datastream
# topics_by_day_ds.print()
# endregion


# Execute the flink streaming envoronment
env.execute(os.path.splitext(os.path.basename(__file__))[0])

