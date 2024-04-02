#!/usr/bin/env python

# A GHArchive file is read and acts as a Kafka producer into the kafka topic "raw-events"
# Template: https://developer.confluent.io/get-started/python/#build-producer 

import sys, json, time
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer


if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            pass
            ## No need to print the items read from the file to the terminal
            # print("\nProduced event to topic {topic}: value = {value:12}".format(
            #     topic=msg.topic(), value=msg.value().decode('utf-8')))

    filepath = '/github-data/2015-01-01-15.json'
   
    topic = 'raw-events'
    
    print("Calculating the number of JSON objects of the file...")
    
    # Calculate size of file (number of lines of file)
    with open(filepath, 'r') as file_object:
            linesInFile = len(file_object.readlines())
    
    linesRead = 0

    print("Reading lines of the file until keyboard interrupt...")
    # Read lines of file to be added in kafka topic until keyboard interrupt
    try:
        for i in range(linesInFile):
                with open(filepath, 'r') as file_object:
                        lines = file_object.readlines()
                        # JSON object to be inserted in the Cassandra database
                        jsonDict = json.loads(lines[i])
                        jsonStr = str(jsonDict)
                        sys.stdout.write("\r JSON objects produced: {0}/{1}".format(i+1, linesInFile))
                        sys.stdout.flush()
                        producer.produce(topic, value=jsonStr, callback=delivery_callback)
                        linesRead = i
                        # # Short time before next JSON object is received
                        # time.sleep(3)
    except KeyboardInterrupt:
        # Each line corresponds to a JSON object (linesRead = numberOfJSONObjectsRead)
        print("\nNumber of JSON objects read from file: %d" % (linesRead))        


    # Block until the messages are sent (wait for 10000 seconds).
    producer.poll(10000)
    # Wait for all messages in the Producer queue to be delivered
    producer.flush()
 
         