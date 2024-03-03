#!/usr/bin/env python

# Reads events from the kafka events-topic and passes it to the cassandra table mykeyspace.events 
# Template: https://developer.confluent.io/get-started/python/#build-consumer


import sys
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
    topic = "events-topic"
    consumer.subscribe([topic], on_assign=reset_offset)

    cassandra_container_name = 'cassandra'
    # Create a Cassandra cluster, connect to it and use a keyspace
    # cluster = Cluster()
    cluster = Cluster([cassandra_container_name],port=9042)

    # A keyspace must have been created before running
    # session = cluster.connect('mykeyspace')
    session = cluster.connect('mykeyspace')
    session.execute('USE mykeyspace')

    # Create the table events 
    session.execute('CREATE TABLE IF NOT EXISTS events (id text PRIMARY KEY, type text, \
                    actor text, repo text, payload text, public text, created_at timestamp)')

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
                # Extract the (optional) key and value, and insert the json object into Cassandra.


                # JSON object to be inserted in the Cassandra database
                jsonBytes = msg.value()
                jsonDict = eval(jsonBytes)
                # Insert fields from the JSON object in the events table
                session.execute("INSERT INTO events (id, type, actor, repo, payload, \
                                public, created_at) \
                                VALUES (%s, %s, %s, %s, %s, %s, %s)", \
                                [str(jsonDict["id"]), str(jsonDict["type"]), str(jsonDict["actor"]), \
                                str(jsonDict["repo"]), str(jsonDict["payload"]), str(jsonDict["public"]), \
                                str(jsonDict["created_at"])])
                        

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