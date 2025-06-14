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
from confluent_kafka import Consumer, OFFSET_BEGINNING, TopicPartition
# from cassandra.cluster import Cluster
from flask import Flask, render_template, request
from random import random
import json

from   sys import exit
import sys

from numpy import empty

from apps.config import config_dict
from apps import create_app, db




"""
Background Thread
"""
forks_and_stars_thread = None
forks_and_stars_thread_lock = Lock()

num_of_raw_events_thread = None
num_of_raw_events_thread_lock = Lock()



##############################################################################
# Stars and forks 
##############################################################################

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

# Set up a callback to handle the '--reset' flag.
def reset_offset(consumer, partitions):
    if args.reset:
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        consumer.assign(partitions)

# Subscribe to topic
topic = "near-real-time-stars-forks"
forks_and_stars_consumer.subscribe([topic], on_assign=reset_offset)


def forks_and_stars_background_thread():

    try:
        while True:
            msg = forks_and_stars_consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting... for stars and forks")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                ## Print consumed events upon receive
                # print("\nConsumed an event from topic {topic}: value = {value:12}".format(
                #     topic=msg.topic(), value=msg.value().decode('utf-8')))
                
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
                
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        forks_and_stars_consumer.close()



##############################################################################
# Count the number of events per second
##############################################################################

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
raw_events_topic_name = "near-real-time-raw-events-ordered"
raw_events_topic = TopicPartition(raw_events_topic_name, 0)
raw_events_consumer.subscribe([raw_events_topic_name], on_assign=reset_offset)


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
                else:
                    timestamp_queue[timestamp] = 1
                    
                # Emit <number_of_timestamps_to_emit_at_once> records once queue is full
                if len(timestamp_queue.items()) >= queue_max_size + number_of_timestamps_to_emit_at_once:                    
                    for i in range(number_of_timestamps_to_emit_at_once):
                        earliest_inserted_key = next(iter(timestamp_queue))
                        num_of_events_on_timestamp = timestamp_queue.pop(earliest_inserted_key)
                        socketio.emit("updateNumOfNearRealTimeRawEvents", {"num_of_events_per_sec": \
                            num_of_events_on_timestamp, "timestamp": earliest_inserted_key})
                        print(f"Popping: 'timestamp: {earliest_inserted_key}, num_of_events_per_sec: {num_of_events_on_timestamp}")
                        
                    socketio.sleep(number_of_timestamps_to_emit_at_once)
                    
                
    except KeyboardInterrupt:
        pass
    finally:
        raw_events_consumer.close()



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
    print('Client connected')

    with num_of_raw_events_thread_lock:
        if num_of_raw_events_thread is None:
            num_of_raw_events_thread = socketio\
                .start_background_task(num_of_raw_events_background_thread)
                
    with forks_and_stars_thread_lock:
        if forks_and_stars_thread is None:
            forks_and_stars_thread = socketio\
                .start_background_task(forks_and_stars_background_thread)

    


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
