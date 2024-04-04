#!/usr/bin/env python

# Reads events from the kafka events-topic and passes it to the cassandra table mykeyspace.events 
# Template: https://developer.confluent.io/get-started/python/#build-consumer

import time, json
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from cassandra.cluster import Cluster

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['consumer-port'])
    config.update(config_parser['consumer'])

    # Create Consumer instance
    consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    topic = "raw-events"
    consumer.subscribe([topic], on_assign=reset_offset)

    cassandra_container_name = 'cassandra'
    # Create a Cassandra cluster, connect to it and use a keyspace
    # cluster = Cluster()
    
    # cluster = Cluster([cassandra_container_name],port=9042)
    cluster = Cluster(contact_points=[cassandra_container_name], port=9042)

    # A keyspace must have been created before running
    # session = cluster.connect('mykeyspace')
    session = cluster.connect()

    session.execute("CREATE KEYSPACE IF NOT EXISTS mykeyspace \
                     WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy'};")
    session.execute('USE mykeyspace')

    # Create type:
    session.execute('CREATE TYPE IF NOT EXISTS actor_type ( \
                    id int, \
                    login text, \
                    display_login text, \
                    gravatar_id text, \
                    url text, \
                    avatar_url text)')


    session.execute('CREATE TYPE IF NOT EXISTS repo_type ( \
                    id int, \
                    name text, \
                    url text)')
    
    session.execute('CREATE TYPE IF NOT EXISTS org_type ( \
                    id int, \
                    login text, \
                    gravatar_id text, \
                    url text, \
                    avatar_url text)')
    
    # Create the table events 
    # The payload field is an object of different type for different event types 
    # (PushEvent, CreateEvent etc) but is assigned the text type for simplicity 
    # for the time being.
    session.execute('CREATE TABLE IF NOT EXISTS events (id text PRIMARY KEY, type text, \
                    actor actor_type, repo repo_type, payload text, public boolean, \
                    created_at text, org org_type)')

    # Delete all pre-existing data of the table events
    session.execute('TRUNCATE events')

    # Poll for new messages from Kafka and insert them in Cassandra
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
                

                # JSON object to be inserted in the Cassandra database
                jsonBytes = msg.value()                
                # Turned into dictionary
                jsonDict = eval(jsonBytes) 
                
                
                # Stringify the payload property of the json object
                jsonDict["payload"] = str(jsonDict["payload"])

                # Stringify the rest of the properties
                jsonDictStringified = json.dumps(jsonDict)
                
                insert_prepared = session.prepare('INSERT INTO events \
                                JSON ?')
                
                # Insert fields from the JSON object in the events table
                session.execute(insert_prepared, [jsonDictStringified])
                

                # time.sleep(3)
                ## Print consumed events upon receive
                print("\nConsumed an event from topic {topic}: value = {value:12}".format(
                    topic=msg.topic(), value=msg.value().decode('utf-8')))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
        # Close all connections from all sessions in Cassandra 
        cluster.shutdown()