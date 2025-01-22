# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

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

from apps.config import config_dict
from apps import create_app, db


"""
Background Thread
"""
thread = None
thread_lock = Lock()


# app = Flask(__name__)
# app.config['SECRET_KEY'] = 'donsky!'





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
config.update(config_parser['near_real_time_stars_forks_consumer'])
    
# Create Consumer instance
consumer = Consumer(config)

# Set up a callback to handle the '--reset' flag.
def reset_offset(consumer, partitions):
    if args.reset:
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        consumer.assign(partitions)

# Subscribe to topic
topic = "near-real-time-stars-forks"
consumer.subscribe([topic], on_assign=reset_offset)


def background_thread():

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                ## Print consumed events upon receive
                print("\nConsumed an event from topic {topic}: value = {value:12}".format(
                    topic=msg.topic(), value=msg.value().decode('utf-8')))
                

                # Returned value to dict to list to dict of language_name to Language object
                jsonBytes = msg.value()
                # print(jsonBytes)
                jsonDict = json.loads(jsonBytes)
                # jsonDict = ast.literal_eval(jsonBytesDecoded)
                
                username = jsonDict["username"]        
                event_type = jsonDict["event_type"]
                repo_name = jsonDict["repo_name"]
                timestamp = jsonDict["timestamp"]
                
                
                # TODO: emit correct fields from topic near-real-time-stars-forks
                # TODO: change the arguments of the function updateNearRealTimeStarsForks 
                # TODO: Make the arguments appear in the changing list
                    
                socketio.emit('updateNearRealTimeStarsForks', {'username': username, 'event_type': event_type, \
                                'repo_name': repo_name, 'timestamp': timestamp})
                  
                # socketio.emit('updateNearRealTimeStarsForks', {'username': username, 'event_type': event_type, \
                #                 'repo_name': repo_name, 'timestamp': timestamp})
                socketio.sleep(1)
                
                time.sleep(3)
                
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()



# WARNING: Don't run with debug turned on in production!
DEBUG = (os.getenv('DEBUG', 'False') == 'True')

# The configuration
get_config_mode = 'Debug' if DEBUG else 'Production'

try:

    # Load the configuration using the default values
    app_config = config_dict[get_config_mode.capitalize()]

except KeyError:
    exit('Error: Invalid <config_mode>. Expected values [Debug, Production] ')

app = create_app(app_config)
Migrate(app, db)
socketio = SocketIO(app, cors_allowed_origins='*')

if not DEBUG:
    Minify(app=app, html=True, js=False, cssless=False)
    
if DEBUG:
    app.logger.info('DEBUG            = ' + str(DEBUG)             )
    app.logger.info('Page Compression = ' + 'FALSE' if DEBUG else 'TRUE' )
    app.logger.info('DBMS             = ' + app_config.SQLALCHEMY_DATABASE_URI)
    app.logger.info('ASSETS_ROOT      = ' + app_config.ASSETS_ROOT )

"""
Serve root index file
"""
@app.route('/')
def index():
    return render_template('login.html')


    
    
"""
Decorator for connect
"""
@socketio.on('connect')
def connect():
    global thread
    print('Client connected')

    global thread
    with thread_lock:
        if thread is None:
            thread = socketio.start_background_task(background_thread)


"""
Decorator for disconnect
"""
@socketio.on('disconnect')
def disconnect():
    print('Client disconnected',  request.sid)



if __name__ == "__main__":
    socketio.run(app, allow_unsafe_werkzeug=True)
    # app.run()