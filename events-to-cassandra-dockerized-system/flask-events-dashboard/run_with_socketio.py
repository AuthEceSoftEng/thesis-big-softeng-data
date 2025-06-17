# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from datetime import datetime
import re
import os
from   flask_migrate import Migrate
from   flask_minify  import Minify
from flask_socketio import SocketIO
from flask import Flask
from threading import Lock
import time

from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING, TopicPartition, OFFSET_END
from cassandra.cluster import Cluster
from flask import Flask, render_template, request
from random import random
import json

from   sys import exit
import sys

from numpy import empty

from apps.config import config_dict
from apps import create_app, db





# Background Threads
# region
forks_and_stars_thread = None
forks_and_stars_thread_lock = Lock()

num_of_raw_events_thread = None
num_of_raw_events_thread_lock = Lock()

query_live_stats_thread = None
query_live_stats_thread_lock = Lock()

# endregion


# Thread 1. Stars and forks 
# region

# Parse the command line.
parser = ArgumentParser()
parser.add_argument('config_file', type=FileType('r'))
parser.add_argument('--reset', action='store_true')
args = parser.parse_args()

# Parse the configuration.
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
config_parser = ConfigParser()
config_parser.read_file(args.config_file)
config = dict(config_parser['default_consumer'])
config.update(config_parser['near_real_time_stars_forks_consumer_2'])
    
# Create Consumer instance
forks_and_stars_consumer = Consumer(config)


# # Set up a callback to handle the '--reset' flag.
def reset_offset_stars_forks(consumer, partitions):
    if args.reset:
        for p in partitions:
            # p.offset = OFFSET_BEGINNING
            p.offset = OFFSET_END
        consumer.assign(partitions)
        


# Subscribe to topic
near_real_time_stars_forks_topic = "near-real-time-stars-forks"
forks_and_stars_consumer.subscribe([near_real_time_stars_forks_topic], 
                on_assign=reset_offset_stars_forks)
# Topic 'near-real-time-stars-forks' has a single partition 
# (to ensure order of messages)
stars_and_forks_partition = TopicPartition(near_real_time_stars_forks_topic, partition=0, offset=OFFSET_END)
forks_and_stars_consumer.assign([stars_and_forks_partition])


def forks_and_stars_background_thread():

    try:
        end_offset = None
        while True:
            # Consume latest star/fork record
            new_end_offset = forks_and_stars_consumer.get_watermark_offsets(stars_and_forks_partition)[1]
            # Seek updates the partition offset according to what is 
            forks_and_stars_consumer.seek(stars_and_forks_partition)
            # print(f"end_offset: {end_offset}, new_end_offset: {new_end_offset}")
            
            
            msg = forks_and_stars_consumer.poll(1.0)
            # Θπον initialization end_offset == None, print the first star/fork event
            if msg is None or (new_end_offset == end_offset):
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting... for stars and forks")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                
                jsonBytes = msg.value()
                jsonDict = json.loads(jsonBytes)
                username = jsonDict["username"]        
                event_type = jsonDict["event_type"]
                repo_name = jsonDict["repo_name"]
                timestamp = jsonDict["timestamp"]
                
                socketio.emit("updateNearRealTimeStarsForks", {"username": username, "event_type": event_type, \
                                "repo_name": repo_name, "timestamp": timestamp})
                socketio.sleep(1)    
                time.sleep(2)

            # The end offset becomes the "new" end offset
            end_offset = new_end_offset
                
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        forks_and_stars_consumer.close()


# endregion



# Thread 2. Count the number of events per second
# region

# Parse the configuration.
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
# config_parser = ConfigParser()
# config_parser.read_file(args.config_file)
count_events_per_sec_config = config

count_events_per_sec_config.update(config_parser['num_of_raw_events_consumer_4'])
    
# Create Consumer instance
raw_events_consumer = Consumer(count_events_per_sec_config)

# Set up a callback to handle the '--reset' flag.
def reset_offset(consumer, partitions):
    if args.reset:
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        consumer.assign(partitions)


# Subscribe to topic near-real-time-raw-events to count the events per topic
raw_events_ordered_topic_name = "near-real-time-raw-events-ordered"
raw_events_topic = TopicPartition(raw_events_ordered_topic_name, 0)
raw_events_consumer.subscribe([raw_events_ordered_topic_name], on_assign=reset_offset)


def num_of_raw_events_background_thread():

    # Stores {timestamp: number_of_events_on_timestamp} 
    timestamp_queue = {}
    queue_max_size = 10
    number_of_timestamps_to_emit_at_once = 2
    raw_events_consumer.assign([raw_events_topic])

    try:
        while True:
            msg = raw_events_consumer.poll(1.0)
            if msg is None:
                print("Waiting... for num of events")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                
                kafka_record = eval(msg.value())
                # print(kafka_record)
                timestamp = kafka_record["created_at"]
                if timestamp in timestamp_queue:
                    timestamp_queue[timestamp] += 1
                elif timestamp not in timestamp_queue:
                    
                    if timestamp_queue != {}:
                        earliest_timestamp_in_queue = next(iter(timestamp_queue))
                        earliest_timestamp_in_queue_as_date = datetime.fromisoformat(earliest_timestamp_in_queue.rstrip('Z')).\
                            strftime('%H:%M:%S')
                        timestamp_as_date = datetime.fromisoformat(timestamp.rstrip('Z')).\
                            strftime('%H:%M:%S')
                        if timestamp_as_date < earliest_timestamp_in_queue_as_date:
                            # # Comment out for debugging
                            # print(f"Got timestamp {timestamp} earlier than earliest timestamp {earliest_timestamp_in_queue}. Ignoring it...")
                            # socketio.sleep(5)
                            
                            continue # Not taking into account timestamps that are earlier than the earliest timestamp in queue
                    
                    timestamp_queue[timestamp] = 1
                    
                    
                # Emit <number_of_timestamps_to_emit_at_once> records once queue is full
                if len(timestamp_queue.items()) >= queue_max_size + number_of_timestamps_to_emit_at_once:                    
                    for i in range(number_of_timestamps_to_emit_at_once):
                        earliest_inserted_key = next(iter(timestamp_queue))
                        num_of_events_on_timestamp = timestamp_queue.pop(earliest_inserted_key)
                        socketio.emit("updateNumOfNearRealTimeRawEvents", {"num_of_events_per_sec": \
                            num_of_events_on_timestamp, "timestamp": earliest_inserted_key})
                        print(f"Popping: timestamp: {earliest_inserted_key}, num_of_events_per_sec: {num_of_events_on_timestamp}")
                        
                    socketio.sleep(number_of_timestamps_to_emit_at_once)
                    
                    
                
    except KeyboardInterrupt:
        pass
    finally:
        raw_events_consumer.close()

# endregion



# Thread 3. Add live statistics every 5 seconds
# region
def query_live_stats_background_thread():

    # Query Cassandra for the live statistics
    cassandra_host = 'cassandra_stelios'
    cassandra_port = 9142
    cluster = Cluster([cassandra_host],port=cassandra_port)
    session = cluster.connect()
    cassandra_keyspace = "near_real_time_data"
    stats_table = "stats_by_day"
    
    select_stats_prepared_query = session.prepare(\
                f"SELECT day, commits, stars, forks, pull_requests "\
                f"FROM {cassandra_keyspace}.{stats_table} WHERE day = ?")
    day = "2025-06-16"
    
    
    
    popular_languages_table = "most_popular_languages_by_day"
    select_langs_prepared_query = session.prepare(\
            f"SELECT language, num_of_occurrences "\
            f"FROM {cassandra_keyspace}.{popular_languages_table} WHERE day = ?")
    
    popular_topics_table = "most_popular_topics_by_day"
    select_topics_prepared_query = session.prepare(\
                    f"SELECT day, topic, num_of_occurrences "\
                    f"FROM {cassandra_keyspace}.{popular_topics_table} WHERE day = ?")
   
    
    # most_popular_repos_table = "most_popular_repos_by_day"

    # select_repos_prepared_query = session.prepare(\
    #         f"SELECT day, repo, stars, forks "\
    #         f"FROM {cassandra_keyspace}.{most_popular_repos_table} WHERE day = ?")
    # repos_queried_res = session.execute(select_repos_prepared_query, [day])            
    # repos_queried_rows = repos_queried_res.all()
    # repos_queried_rows_sorted_by_stars = sorted(repos_queried_rows, key=lambda x: x.stars, reverse=True)
    # repos_queried_rows_sorted_by_forks = sorted(repos_queried_rows, key=lambda x: x.forks, reverse=True)
    
        
    try:
        while True:
            stats_queried_res = session.execute(select_stats_prepared_query, [day])           
            stats_queried_row = stats_queried_res.one()
    
    
            langs_queried_res = session.execute(select_langs_prepared_query, [day])            
            langs_queried_rows = langs_queried_res.all()
            langs_queried_rows_sorted = sorted(langs_queried_rows, key=lambda x: x.num_of_occurrences, reverse=True)
            print(f"Languages on {day}:\n"\
                    "Language\tNumber of occurrences")
            
            max_num_of_langs_to_emit = 5
            langs_queried_rows_sorted_keep_top_ranked = langs_queried_rows_sorted[0:max_num_of_langs_to_emit]
            langs_queried_names = [lang_row.language for lang_row in langs_queried_rows_sorted_keep_top_ranked]
            langs_queried_num_of_occurrences = [lang_row.num_of_occurrences for lang_row in langs_queried_rows_sorted_keep_top_ranked]
    
    
            topics_queried_res = session.execute(select_topics_prepared_query, [day])            
            topics_queried_rows = topics_queried_res.all()
            topics_queried_rows_sorted = sorted(topics_queried_rows, key=lambda x: x.num_of_occurrences, reverse=True)
            
            max_num_of_topics_to_emit = 5
            topics_queried_rows_sorted_keep_top_ranked = topics_queried_rows_sorted[0:max_num_of_topics_to_emit]
            topics_queried_names = [topic_elem.topic for topic_elem in  topics_queried_rows_sorted_keep_top_ranked]
            topics_queried_num_of_occurrences =[topic_elem.num_of_occurrences for topic_elem in  topics_queried_rows_sorted_keep_top_ranked]
            
            
            
            print(f"Topics on {day}:\n"\
                    "Topic\tNumber of occurrences")
            for i in range(len(topics_queried_rows_sorted_keep_top_ranked)):
                    print(f"{topics_queried_rows_sorted_keep_top_ranked[i].topic}\t{topics_queried_rows_sorted_keep_top_ranked[i].num_of_occurrences}")
            print()
            
    
    
            # Live data socketio.emitted logs 
            # print(f"Stats on {day}:\n"\
            #     f"Day:\tCommits\tStars\tForks\tPull requests\n"
            #     f"{stats_queried_row.day}\t"\
            #     f"{stats_queried_row.commits}\t"\
            #     f"{stats_queried_row.stars}\t"
            #     f"{stats_queried_row.forks}\t"
            #     f"{stats_queried_row.pull_requests}\n")
            
            # for i in range(len(langs_queried_rows_sorted_keep_top_ranked)):
            #         print(f"{langs_queried_rows_sorted_keep_top_ranked[i].language}\t{langs_queried_rows_sorted_keep_top_ranked[i].num_of_occurrences}")
            # print()
    
    
            # Socket emit the live data
            socketio.emit("updateRealTimeStats", 
                            {"day": day,
                                "stats": {
                                    "commits": stats_queried_row.commits,
                                    "stars": stats_queried_row.stars,
                                    "forks": stats_queried_row.forks,
                                    "pull_requests": stats_queried_row.pull_requests    
                                },
                                "most_popular_languages": {
                                    "lang_names": langs_queried_names,
                                    "num_of_occurrences": langs_queried_num_of_occurrences
                                },
                                "most_popular_topics": {
                                    "topic_names": topics_queried_names,
                                    "num_of_occurrences": topics_queried_num_of_occurrences
                                }
                            }
                        )
            time_between_stats_emit = 5
            socketio.sleep(time_between_stats_emit)
            
            
        
            
            
            
    finally:
        cluster.shutdown()
        print("Cluster was shutdown")
        


# endregion




# # WARNING: Don't run with debug turned on in production!
# DEBUG = (os.getenv('DEBUG', 'False') == 'True')
DEBUG = False

# The configuration
get_config_mode = 'Debug' if DEBUG else 'Production'

try:

    # Load the configuration using the default values
    app_config = config_dict[get_config_mode.capitalize()]

except KeyError:
    exit('Error: Invalid <config_mode>. Expected values [Debug, Production] ')
    
    

app = create_app(app_config)
# app = Flask(__name__)
Migrate(app, db)


"""
Serve root index file
"""
@app.route('/')
def index():
    return render_template('login.html')



socketio = SocketIO(app, cors_allowed_origins='*')
# socketio = SocketIO(app)

# # if not DEBUG:
# Minify(app=app, html=True, js=False, cssless=False)
    
# if DEBUG:
#     app.logger.info('DEBUG            = ' + str(DEBUG)             )
#     app.logger.info('Page Compression = ' + 'FALSE' if DEBUG else 'TRUE' )
#     app.logger.info('DBMS             = ' + app_config.SQLALCHEMY_DATABASE_URI)
#     app.logger.info('ASSETS_ROOT      = ' + app_config.ASSETS_ROOT )

    
"""
Decorator for connect
"""
@socketio.on('connect')
def connect():
    global forks_and_stars_thread
    global num_of_raw_events_thread
    global query_live_stats_thread
    
    print('Client connected')

    with num_of_raw_events_thread_lock:
        if num_of_raw_events_thread is None:
            num_of_raw_events_thread = socketio\
                .start_background_task(num_of_raw_events_background_thread)
                
    with forks_and_stars_thread_lock:
        if forks_and_stars_thread is None:
            forks_and_stars_thread = socketio\
                .start_background_task(forks_and_stars_background_thread)

    with query_live_stats_thread_lock:
        if query_live_stats_thread is None:
            query_live_stats_thread = socketio\
                .start_background_task(query_live_stats_background_thread)
    
    


"""
Decorator for disconnect
"""
@socketio.on('disconnect')
def disconnect():
    print('Client disconnected',  request.sid)



if __name__ == "__main__":
    # print("The code in main started")
    socketio.run(app, host="0.0.0.0", allow_unsafe_werkzeug=True, port=5100)
    # socketio.run(app)
    # app.run()
