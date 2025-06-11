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
from confluent_kafka import Consumer, OFFSET_BEGINNING
# from cassandra.cluster import Cluster
from flask import Flask, render_template, request
from random import random
import json

from   sys import exit

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
                print("Waiting...")
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

count_events_per_sec_config.update(config_parser['num_of_raw_events_consumer'])
    
# Create Consumer instance
raw_events_consumer = Consumer(count_events_per_sec_config)

# Set up a callback to handle the '--reset' flag.
def reset_offset(consumer, partitions):
    if args.reset:
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        consumer.assign(partitions)


# Subscribe to topic near-real-time-raw-events to count the events per topic
raw_events_topic = "near-real-time-raw-events"
raw_events_consumer.subscribe([raw_events_topic], on_assign=reset_offset)


def num_of_raw_events_background_thread():
    
    # Create a dict of (timestamp, number of events with the timestamp) 
    # and expose it on socket emit
    
    timestamp_to_number_of_events_dict = dict()
    last_msg_offset = None
    try:
        previous_timestamp = None
        while True:
            msg = raw_events_consumer.poll(1.0)
            last_msg_offset = msg
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # On the first incoming non-None message print the offset
                if previous_timestamp == None:
                    print(f'Started from offset: {msg.offset()}')
                
                ## Print consumed events upon receive
                # print("\nConsumed an event from topic: {topic},  value = {value:12}".format( \
                #     topic=msg.topic(), value=msg.value().decode('utf-8')))
                
                
                jsonBytes = msg.value()
                # jsonBytes = str(msg.value()).replace("'", '"')
                # print(f"From thread: 'num_of_raw_events_background_thread': jsonbytes type: {type(jsonBytes)}")
                jsonDict = eval(jsonBytes)                
                timestamp = jsonDict["created_at"]
                
                
                
                # On the first timestamp, do not emit (timestamp, number of events on timestamp) 
                if timestamp not in timestamp_to_number_of_events_dict.keys() \
                    and timestamp_to_number_of_events_dict == {}:
                    timestamp_to_number_of_events_dict[timestamp] = 1
                    previous_timestamp = timestamp
                
                
                # If the consumed timestamp exists, increase its value by one
                elif timestamp in timestamp_to_number_of_events_dict.keys():
                    timestamp_to_number_of_events_dict[timestamp] += 1
                
                
                # If the timestamp does not exist and is not the first emitted timestamp,
                # create a dict row with 'number of events on timestamp' = one
                # and emit the number of events of the previous timestamp
                # (Heuristic) Also, if the previous number of occurences is 1, then we omit it,
                # as it is probably a not in-order message.
                elif timestamp not in timestamp_to_number_of_events_dict.keys() \
                    and timestamp != None and \
                        timestamp_to_number_of_events_dict[previous_timestamp] != 1:
                
                    # If the next timestamp is before the previous one, do not emit it and do not change 
                    # the previous timestamp with the new one
                    # (continue to the next iteration of the loop) (we want the emitted timestamps and 
                    # number of events to be sequential. This intρoduces a negligible error on the number of events 
                    # per timestamp)
                    # TODO: Possible problem is the fact that the consumer may start from a date much later
                    # than the first one. Example: if the first one is 
                    # 2024-08-23T14:00:00Z
                    # and the emitted one is 
                    time_only = datetime.fromisoformat(timestamp.rstrip('Z')).strftime('%H:%M:%S')
                    previous_time_only = datetime.fromisoformat(previous_timestamp.rstrip('Z')).\
                        strftime('%H:%M:%S')
                    if time_only < previous_time_only:
                        continue    
                    
                    
                    # Debugging: Print the new timestamp and its number of events
                    print(f"Previous timestamp: {previous_timestamp}, "
                        f"Num of events:"
                        f"{timestamp_to_number_of_events_dict[previous_timestamp]}")
                    
                    # Add the new timestamp to the dictionary
                    timestamp_to_number_of_events_dict[timestamp] = 1
                    
                    # Emit the old timestamp
                    socketio.emit("updateNumOfNearRealTimeRawEvents", {"num_of_events_per_sec": \
                        timestamp_to_number_of_events_dict[previous_timestamp], "timestamp": previous_timestamp})
                    
                    # After emitting the previous timestamp, 
                    # turn the current timestamp into the previous one
                    previous_timestamp = timestamp
                    socketio.sleep(1)
                
                # time.sleep(2)
                
    except KeyboardInterrupt:
        pass
    finally:
        print(f"Consumed messages up to offset: {last_msg_offset.offset()}")
        # Leave group and commit final offsets
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
