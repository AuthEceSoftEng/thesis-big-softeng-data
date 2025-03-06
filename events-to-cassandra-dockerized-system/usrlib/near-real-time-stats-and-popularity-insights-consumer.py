'''
Consumes near-real time events from kafka topics and stores it into Cassandra tables:
Datastream                                  Consumed and stored into Cassandra table

D1_2_3 from topic 'stats-by-day'            T1_2: 'gharchive.stats_by_day'
-----------/ /-----------                   T3: 'gharchive.forks_by_day'
D4 from topic 'popular-languages-by-day'    T4: 'gharchive.popular_languages_by_day'
D5 from topic 'popular-topics-by-day'       T5: 'gharchive.popular_topics_by_day'
'''


# Reads events' repos from the kafka events-topic and passes it to the cassandra table mykeyspace.repos
# Template: https://deve`lo`per.confluent.io/get-started/python/#build-consumer


from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING, OFFSET_STORED
from cassandra.cluster import Cluster

from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args

from argparse import ArgumentParser, RawDescriptionHelpFormatter
from configparser import ConfigParser

import sys
import json


# Consumer that gets datastream elements from datastream D1_2_3 and feeds it into T1_2 and T3
def consume_events_and_store_stats_in_tables_concurrently():
    '''
    For all events in kafka topic "stats_by_day", extracts fields 
    (day, repo_name, commits, open_issues, closed_issues
	stars, forks, pull-requests)
 
    '''
    
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()


    # Parse the configuration to setup the consumer
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default_consumer'])
    config.update(config_parser['store_stats_by_day_to_cassandra_consumer'])

    # print(config)
    # exit()
    
    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):    
        for p in partitions:    
            p.offset = OFFSET_STORED
        consumer.assign(partitions)

    # Create Consumer instance
    consumer = Consumer(config)

    # Subscribe to topic
    topic = 'stats_by_day'
    
    # Consumer consuming from where it left off
    consumer.subscribe([topic], on_assign=reset_offset)

    topic_partition = TopicPartition(topic, partition=0)

    

    # Create a Cassandra cluster, connect to it and use a keyspace
    # cluster = Cluster()
    cassandra_container_name = 'cassandra'
    cluster = Cluster([cassandra_container_name],port=9142)
    
    keyspace = 'near_real_time_gharchive'
    create_keyspace = "CREATE KEYSPACE IF NOT EXISTS "\
    f"{keyspace} WITH replication = {{'class': 'SimpleStrategy', "\
    f"'replication_factor': '1'}} AND durable_writes = true;"
    session = cluster.connect()
    session.execute(create_keyspace) 
    session = cluster.connect(keyspace)
    session.execute(f'USE {keyspace}')
                    
    
    # Create table 'stats_by_day': T1_2 
    create_table_t1_2_query = " \
    CREATE TABLE IF NOT EXISTS stats_by_day \
        (day text, repo_name text, stars int, forks int, commits int, \
        pull_requests int, open_issues int, closed_issues int, \
        PRIMARY KEY ((day), stars, repo_name))  \
        WITH comment = 'T1_2: Q1: Get total number of stars, forks, \
        commits and pull_requests by day, Q2: Find most starred \
        repos by day' AND CLUSTERING ORDER BY (stars DESC);\
    "
    session.execute(create_table_t1_2_query)
    
    
    # Create table 'forks_by_day': T1_2 
    create_table_t_3_query = " \
    CREATE TABLE IF NOT EXISTS forks_by_day \
        (day text, repo_name text, stars int, forks int, \
        PRIMARY KEY ((day), forks, repo_name))  \
        WITH comment = 'T3: Q3: Find most forked repos by day' \
        AND CLUSTERING ORDER BY (forks DESC);\
    "
    session.execute(create_table_t_3_query)
    
    
     
    topic_partition_info = consumer.committed([topic_partition], timeout=5)[0]
    print(f"Consumer of {topic} left off in offset: {topic_partition_info.offset}")

    # Poll for new messages from Kafka and insert them in Cassandra
    try:
        while True :
            msgs = consumer.consume(num_messages = 10000, timeout = 5)
            
            if msgs == []:    

                    # # Initial message consumption may take up to
                    # # `session.timeout.ms` for the consumer group to
                    # # rebalance and start consuming
                    # print("Waiting for new messages...")
                    print(f"Either no more messages left to consume from topic {topic}\n"
                          "or empty list of messages returned due to timeout.\n"
                          f"Shutting down consumer of topic {topic}")
                    break
                    
            # elif msgs.error():
            #     print("ERROR: %s".format(msgs.error()))
            else:
               
                # Print the offsets of the consumed messages
                last_consumed_message = msgs[-1]
                offset_of_last_consumed_message = last_consumed_message.offset()
                sys.stdout.write(f"\rConsumed events up to offset: {offset_of_last_consumed_message}\t")
                sys.stdout.flush()
                
                # Extract only the fields needed from the raw events
                json_bytes_list = [msg.value() for msg in msgs]
                json_dict_list = [json.loads(json_bytes.decode('utf-8')) for json_bytes in json_bytes_list]

                # Make a list out of the messages
                day_list = [json_dict["day"] for json_dict in json_dict_list]
                repo_name_list = [json_dict["repo_name"]  for json_dict in json_dict_list]                
                commits_list = [json_dict["commits"] for json_dict in json_dict_list]
                stars_list = [json_dict["stars"] for json_dict in json_dict_list]
                forks_list = [json_dict["forks"] for json_dict in json_dict_list]
                open_issues_list = [json_dict["open_issues"] for json_dict in json_dict_list]
                closed_issues_list = [json_dict["closed_issues"] for json_dict in json_dict_list]
                pull_requests_list = [json_dict["pull_requests"] for json_dict in json_dict_list]
                    
                # Populate table T1_2: 
                # (day, repo_name, commits, stars, forks, open_issues, closed_issues, pull_requests)
                
                # Batch the data and save it into cassandra 
                batch_data_t1_2 = zip(day_list, repo_name_list, commits_list, stars_list, forks_list,\
                    open_issues_list, closed_issues_list, pull_requests_list)
                batch_data_t1_2 = list(batch_data_t1_2)
                query = "INSERT INTO stats_by_day (day, repo_name, commits, stars, forks, open_issues, \
                         closed_issues, pull_requests) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                execute_concurrent_with_args(session, query, parameters=batch_data_t1_2) 

                # Populate table T3: 
                # (day, repo_name, stars, forks)
                # Batch the data and save it into cassandra 
                batch_data_t3 = zip(day_list, repo_name_list, forks_list, stars_list)
                batch_data_t3 = list(batch_data_t3)
                query = "INSERT INTO forks_by_day (day, repo_name, forks, stars)\
                    VALUES (%s, %s, %s, %s)"
                execute_concurrent_with_args(session, query, parameters=batch_data_t3) 


                    
    except KeyboardInterrupt:
        pass
    finally:

        topic_partition_info = consumer.committed([topic_partition], timeout=5)[0]
        print(f"Last consumed offset: {topic_partition_info.offset}")

        # Leave group and commit final offsets
        consumer.close()
        # Close all connections from all sessions in Cassandra 
        cluster.shutdown()
        print(f"\nConsumer of topic {topic} closed properly")      

# Consumer that gets datastream elements from datastream D4 and feeds it into T4
def consume_events_and_store_language_popularity_in_table_concurrently():
    '''
    For all events in kafka topic "popular_languages_by_day", extracts fields 
    (day, number_of_events, language)
    '''
    
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()


    # Parse the configuration to setup the consumer
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default_consumer'])
    config.update(config_parser['store_popular_languages_by_day_to_cassandra_consumer'])

    # print(config)
    # exit()
    
    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):    
        for p in partitions:    
            p.offset = OFFSET_STORED
        consumer.assign(partitions)

    # Create Consumer instance
    consumer = Consumer(config)

    # Subscribe to topic
    topic = 'popular_languages_by_day'
    
    # Consumer consuming from where it left off
    consumer.subscribe([topic], on_assign=reset_offset)

    topic_partition = TopicPartition(topic, partition=0)

    

    # Create a Cassandra cluster, connect to it and use a keyspace
    # cluster = Cluster()
    cassandra_container_name = 'cassandra'
    cluster = Cluster([cassandra_container_name],port=9142)
    
    keyspace = 'near_real_time_gharchive'
    create_keyspace = "CREATE KEYSPACE IF NOT EXISTS "\
    f"{keyspace} WITH replication = {{'class': 'SimpleStrategy', "\
    f"'replication_factor': '1'}} AND durable_writes = true;"
    session = cluster.connect()
    session.execute(create_keyspace) 
    session = cluster.connect(keyspace)
    session.execute(f'USE {keyspace}')
                    
                    
    
    
    # Create table 'popular_languages_by_day': T4
    create_table_t4_query = f" \
        CREATE TABLE IF NOT EXISTS {keyspace}.popular_languages_by_day \
            (day text, language text, number_of_events int, \
            PRIMARY KEY ((day), number_of_events, language)) \
            WITH comment = 'T4: \
            Q4: Find most used languages by number of repos' \
            AND CLUSTERING ORDER BY  (number_of_events DESC)\
    "
    session.execute(create_table_t4_query)
    
    
     
    topic_partition_info = consumer.committed([topic_partition], timeout=5)[0]
    print(f"Consumer of {topic} left off in offset: {topic_partition_info.offset}")

    # Poll for new messages from Kafka and insert them in Cassandra
    try:
        while True :
            msgs = consumer.consume(num_messages = 10000, timeout = 5)
            
            if msgs == []:    

                    # # Initial message consumption may take up to
                    # # `session.timeout.ms` for the consumer group to
                    # # rebalance and start consuming
                    # print("Waiting for new messages...")
                    print(f"Either no more messages left to consume from topic {topic}\n"
                          "or empty list of messages returned due to timeout.\n"
                          f"Shutting down consumer of topic {topic}")
                    break
                    
            # elif msgs.error():
            #     print("ERROR: %s".format(msgs.error()))
            else:
               
                # Print the offsets of the consumed messages
                last_consumed_message = msgs[-1]
                offset_of_last_consumed_message = last_consumed_message.offset()
                sys.stdout.write(f"\rConsumed events up to offset: {offset_of_last_consumed_message}\t")
                sys.stdout.flush()
                
                # Extract only the fields needed from the raw events
                json_bytes_list = [msg.value() for msg in msgs]
                json_dict_list = [json.loads(json_bytes.decode('utf-8')) for json_bytes in json_bytes_list]

                # Make a list out of the messages
                day_list = [json_dict["day"] for json_dict in json_dict_list]
                language_list = [json_dict["language"]  for json_dict in json_dict_list]                
                
                number_of_events_list = [json_dict["number_of_events"] for json_dict in json_dict_list]
                
                    
                # Populate table T4: 
                # (day, language, number_of_evensts)
                
                # Batch the data and save it into cassandra 
                batch_data_t4 = zip(day_list, language_list, number_of_events_list)
                batch_data_t4 = list(batch_data_t4)
                query = "INSERT INTO popular_languages_by_day (day, language, number_of_events) \
                        VALUES (%s, %s, %s)"
                execute_concurrent_with_args(session, query, parameters=batch_data_t4) 

    except KeyboardInterrupt:
        pass
    finally:

        topic_partition_info = consumer.committed([topic_partition], timeout=5)[0]
        print(f"Last consumed offset: {topic_partition_info.offset}")

        # Leave group and commit final offsets
        consumer.close()
        # Close all connections from all sessions in Cassandra 
        cluster.shutdown()
        print(f"\nConsumer of topic {topic} closed properly")      

# Consumer that gets datastream elements from datastream D5 and feeds it into T5
def consume_events_and_store_topics_popularity_in_table_concurrently():
    '''
    For all events in kafka topic "popular_topics_by_day", extracts fields 
    (day, number_of_events, topic)
    '''
    
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()


    # Parse the configuration to setup the consumer
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default_consumer'])
    config.update(config_parser['store_popular_topics_by_day_to_cassandra_consumer'])

    # print(config)
    # exit()
    
    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):    
        for p in partitions:    
            p.offset = OFFSET_STORED
        consumer.assign(partitions)

    # Create Consumer instance
    consumer = Consumer(config)

    # Subscribe to topic
    topic = 'popular_topics_by_day'
    
    # Consumer consuming from where it left off
    consumer.subscribe([topic], on_assign=reset_offset)

    topic_partition = TopicPartition(topic, partition=0)

    

    # Create a Cassandra cluster, connect to it and use a keyspace
    # cluster = Cluster()
    cassandra_container_name = 'cassandra'
    cluster = Cluster([cassandra_container_name],port=9142)
    # A keyspace must have been created before running
    keyspace = 'near_real_time_gharchive'
    create_keyspace = "CREATE KEYSPACE IF NOT EXISTS "\
    f"{keyspace} WITH replication = {{'class': 'SimpleStrategy', "\
    f"'replication_factor': '1'}} AND durable_writes = true;"
    session = cluster.connect()
    session.execute(create_keyspace) 
    session = cluster.connect(keyspace)
    session.execute(f'USE {keyspace}')
    
    
    # Create table 'popular_languages_by_day': T4
    create_table_t5_query = f" \
        CREATE TABLE IF NOT EXISTS {keyspace}.popular_topics_by_day \
            (day text, topic text, number_of_events int, \
            PRIMARY KEY ((day), number_of_events, topic)) \
            WITH comment = 'T5: \
            Q5: Find most used topics by number of repos' \
            AND CLUSTERING ORDER BY (number_of_events DESC)\
    "
    session.execute(create_table_t5_query)
    
    
    
    topic_partition_info = consumer.committed([topic_partition], timeout=5)[0]
    print(f"Consumer of {topic} left off in offset: {topic_partition_info.offset}")


    # Poll for new messages from Kafka and insert them in Cassandra
    try:
        while True :
            msgs = consumer.consume(num_messages = 10000, timeout = 5)
            
            if msgs == []:    

                    # # Initial message consumption may take up to
                    # # `session.timeout.ms` for the consumer group to
                    # # rebalance and start consuming
                    # print("Waiting for new messages...")
                    print(f"Either no more messages left to consume from topic {topic}\n"
                          "or empty list of messages returned due to timeout.\n"
                          f"Shutting down consumer of topic {topic}")
                    break
                    
            # elif msgs.error():
            #     print("ERROR: %s".format(msgs.error()))
            else:
               
                # Print the offsets of the consumed messages
                last_consumed_message = msgs[-1]
                offset_of_last_consumed_message = last_consumed_message.offset()
                sys.stdout.write(f"\rConsumed events up to offset: {offset_of_last_consumed_message}\t")
                sys.stdout.flush()
                
                # Extract only the fields needed from the raw events
                json_bytes_list = [msg.value() for msg in msgs]
                json_dict_list = [json.loads(json_bytes.decode('utf-8')) for json_bytes in json_bytes_list]

                # Make a list out of the messages
                day_list = [json_dict["day"] for json_dict in json_dict_list]
                topic_list = [json_dict["topic"]  for json_dict in json_dict_list]                
                number_of_events_list = [json_dict["number_of_events"] for json_dict in json_dict_list]
                
                    
                # Populate table T5: 
                # (day, topic, number_of_events)
                
                # Batch the data and save it into cassandra 
                batch_data_t5 = zip(day_list, topic_list, number_of_events_list)
                batch_data_t5 = list(batch_data_t5)
                query = "INSERT INTO popular_topics_by_day (day, topic, number_of_events) \
                        VALUES (%s, %s, %s)"
                execute_concurrent_with_args(session, query, parameters=batch_data_t5) 

    except KeyboardInterrupt:
        pass
    finally:




        topic_partition_info = consumer.committed([topic_partition], timeout=5)[0]
        print(f"Last consumed offset: {topic_partition_info.offset}")

        # Leave group and commit final offsets
        consumer.close()
        # Close all connections from all sessions in Cassandra 
        cluster.shutdown()
        print(f"\nConsumer of topic {topic} closed properly")      

# Consume datastream D1_2_3: 'stats_by_day' and populate tables T1_2: 'stats_by_day' 
# and T3: 'forks_by_day'
consume_events_and_store_stats_in_tables_concurrently()

# Consume datastream D4: 'popular_languages_by_day' and populate table 
# T4: 'popular_languages_by_day'
consume_events_and_store_language_popularity_in_table_concurrently()

# Consume datastream D5: 'popular_topics_by_day' and populate table 
# T5: 'popular_topics_by_day'
consume_events_and_store_topics_popularity_in_table_concurrently()