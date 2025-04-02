'''
Produce messages from last line read of file
'''

# Copy and paste the producer.py script here and make it so
# it saves the filename and the line parsed and when starting again
# to produce from the same file, it starts from the line we left off.
# If the topic was deleted and is only now created, the file 
# content keeping the files (name, line_we_left_off) should also 
# be deleted.

from get_parsed_gharchive_files import save_files_parsed, restore_parsed_files

# A GHArchive file is read and acts as a Kafka producer into the kafka topic "raw-events"
# Template: https://developer.confluent.io/get-started/python/#build-producer 

import sys, json, time, os, gzip
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, admin
import shutil
from pathlib import Path


def extract_compressed_file_from_path(compressedFilePath, do_again=False):
    '''
    Extract a .json.gz into a .json file in the parent folder of the compressedFilePath
    The file is decompressed only if it is not already present
    returns: the filepath of the extracted file
    '''

    parent_folder = str(Path(compressedFilePath).parent)

    decompressed_file_name = os.path.basename(compressedFilePath)
    decompressed_file_name = decompressed_file_name.replace('.json.gz', '')
    decompressed_file_filepath = str(parent_folder) + '/' + decompressed_file_name + '.json'

    print(f"Decompressing {os.path.basename(compressedFilePath)}...",
              end="")
    # If the decompressed file does not exist already, decompress the .json.gz
    if not os.path.exists(decompressed_file_filepath):
        with gzip.open(compressedFilePath, 'rb') as f_in:
            with open(decompressed_file_filepath, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print('Done')
    elif os.path.exists(decompressed_file_filepath) and do_again==True:
        os.remove(decompressed_file_filepath)
        with gzip.open(compressedFilePath, 'rb') as f_in:
            with open(decompressed_file_filepath, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print('Done')
    
    else:
        print("The decompressed file already exists in path {}\n".
        format(decompressed_file_filepath))
    return decompressed_file_filepath

def delivery_callback(err, msg):
    '''Optional per-message delivery callback (triggered by poll() or flush())
    when a message has been successfully delivered or permanently
    failed delivery (after retries).
    '''
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        pass
        ## No need to print the items read from the file to the terminal
        # print("\nProduced event to topic {topic}: value = {value:12}".format(
        #     topic=msg.topic(), value=msg.value().decode('utf-8')))

def create_topic(topic=str, config_port=str):
        '''
        Checks if a topic exists and if it does not, it creates it
        `topic`: topic name 
        `config_port`: the port of the kafka cluster (e.g. localhost:44352)
        '''
        client = admin.AdminClient({"bootstrap.servers": config_port})
        topic_metadata = client.list_topics(timeout=5)
        all_topics_list = topic_metadata.topics.keys()
        
        # If the topic does not exist, delete the contents of the file, 
        # then create it 
        if topic not in all_topics_list:
            print(f'Topic {topic} does not exist')
            new_topic = admin.NewTopic(topic, num_partitions=4, replication_factor=1)
            # client.create_topics([new_topic])
            
            futures = client.create_topics([new_topic])
            for all_topics, future in futures.items():
                try:
                    future.result()  # Wait for topic creation
                    print(f"Topic {topic} created successfully")
                except Exception as e:
                    print(f"Failed to create topic {topic}: {e}")
        else: 
            print(f"Topic {topic} already exists")
                

        
def delete_topic(topic=str, config_port=str):
    '''
    Checks if a topic exists and if it does , 
    it deletes it
    `topic`: topic name 
    `config_port`: the port of the kafka cluster (e.g. localhost:44352)
    returns: True if topic exists or False if it does not
    '''
    client = admin.AdminClient({"bootstrap.servers": config_port})
    topic_metadata = client.list_topics()
    all_topics_list = topic_metadata.topics.keys()
    
    # If the topic does not exist, delete the contents of the file, 
    # then create it 
    if topic not in all_topics_list:
        raise ValueError(f'Cannot delete topic {topic}. The topic does not exist.')
    else:
        client.delete_topics([topic])
        print(f'Topic {topic} was deleted')

                
            
def delete_file_contents(parsed_files_filepath):
    print('Deleting the parsed files contents...\n')
    # Truncate the file to start writing new events to it
    with open(parsed_files_filepath, 'w'):
        pass


def produce_from_line_we_left_off(topic=str, filepath=str, \
    parsed_files_filepath=str, config=dict):
    '''
    Produces messages from a .json file into a topic
    topic: the name of the topic to produce messages into
    filepath: the path to the *.json.gz file to decompress and read from
    parsed_files_dict: dictionary with the files and the line we left off 
    last time we read it
    
    :returns: the_whole_file_was_read:
    True: if the whole file was read 
    False: otherwise (either because of KeyboardInterrupt or because of error)
    
    the_whole_file_was_read_beforehand:
    True: if the whole file was read 
    False: otherwise (either because of KeyboardInterrupt or because of error)
    
    '''
    
    filename = os.path.basename(filepath)
    
    # Decompress gharchive file    
    decompressed_file_path = extract_compressed_file_from_path(\
        filepath)
    
    # Calculate size of file (number of lines of file)    
    # print(f"Calculating the number of Github events in file {os.path.basename(decompressed_file_path)}")
    with open(decompressed_file_path, 'r') as file_object:
        linesInFile = len(file_object.readlines())
    # print(f'Total number of lines in {filename}: {linesInFile}\n')
    
    parsed_files_dict = restore_parsed_files(parsed_files_filepath)
    # Get the lines where we left off for the file and continue from there
    if filename not in parsed_files_dict.keys():
        parsed_files_dict[filename] = 0    
    line_we_left_off = parsed_files_dict[filename]
    

    # Declare variable linesProduced to be used in the loop 
    linesProduced = 0
                
    the_whole_file_was_read = False
    the_whole_file_was_read_beforehand = False
    
    number_of_lines_produced_per_print = 1000
    
    # Read lines of file to be added in kafka topic until keyboard interrupt
    # of EOF
    
    # Case 1: The file has not been read yet 
    # or has been read up to a line before the last one
    if line_we_left_off < linesInFile:
        try:
            # Create Producer instance
            producer = Producer(config)
            with open(decompressed_file_path, 'r') as file_object:
                lines = file_object.readlines()
                print(f"Reading lines of {filename} until EOF or keyboard interrupt...")
                print(f'Producing events from line No.{line_we_left_off+1} of {filename}')
                
                for i in range(line_we_left_off, linesInFile):    
                    # JSON object to be inserted in the Cassandra database
                    jsonDict = json.loads(lines[i])
                    jsonStr = str(jsonDict)
                    
                    if i % number_of_lines_produced_per_print == 0:
                        sys.stdout.write("\rJSON objects produced: {0}/{1}".format(i+1, linesInFile))
                        sys.stdout.flush()
                        
                    # TODO: Produce in batches (see kafka docs)
                    producer.produce(topic, value=jsonStr, callback=delivery_callback)
                    linesProduced = i+1
                    
                    # Poll to cleanup the producer queue after every message production
                    # See: https://stackoverflow.com/questions/62408128/buffererror-local-queue-full-in-python
                    producer.poll(0)
                    # # Time sleep is used here to capture output
                    time.sleep(0.0001)
                        
                sys.stdout.write("\rJSON objects produced: {0}/{1}".format(linesInFile, linesInFile))
                sys.stdout.flush()
                        
        
        # Case 1.1: Keyboard interrupt while reading the file
        except KeyboardInterrupt:
            print('\nKeyboard interrupt\n')
            the_whole_file_was_read = False
        finally:
            # Once done reading the decompressed file, 
            # delete it to save space and save the state
            os.remove(decompressed_file_path)
            parsed_files_dict[filename] = linesProduced      
            save_files_parsed(parsed_files_dict, parsed_files_filepath)
            
            # Block until the messages are sent (wait for 10000 seconds).
            producer.poll(10000)
            # Wait for all messages in the Producer queue to be delivered
            producer.flush()
            print('Producer closed properly')
            
            
        
        # Case 1.2: EOF 
        # In this case, the file was read up to the last line and
        # no keyboard interrupt occured
        if linesProduced == linesInFile:
            print('\nEOF\n')
            # Keyboard interrupt did not occur, the whole file was read 
            the_whole_file_was_read = True
            if os.path.exists(decompressed_file_path):
                # Remove the decompressed file
                os.remove(decompressed_file_path)
            
                # # Save state
                # parsed_files_dict[filename] = linesProduced      
                # save_files_parsed(parsed_files_dict, parsed_files_filepath)
                
                # # Poll and close the producer.
                # producer.poll(10000)
                # producer.flush()
                # print('Producer closed properly')
            
            
            
    # Case 2: If the whole file was read before the execution of the script
    # (line_we_left_off = flinesInFile)
    elif line_we_left_off == linesInFile:
        print('EOF: '
            f'All lines of file {filename} have already been read '
            f'and produced into topic: {topic}')
        the_whole_file_was_read = True   
        the_whole_file_was_read_beforehand = True
        if os.path.exists(decompressed_file_path):
            # Once done reading the decompressed file, delete it to save space 
            os.remove(decompressed_file_path)
            # # Save the state  (state should have already been saved if 
            # # line_we_left_off == linesInFile
            # linesProduced = linesInFile
            # parsed_files_dict[filename] = linesProduced      
            # save_files_parsed(parsed_files_dict, parsed_files_filepath)
            print()
    # Case 3: An error occurred: (lines_we_left_off > linesInFile)
    else:
        raise ValueError(f'Lines we read: ({linesProduced}) should be more than the ones '
                         f'already existing in file {decompressed_file_path} '
                         f'({linesInFile})')
    return the_whole_file_was_read, the_whole_file_was_read_beforehand
   


# if __name__ == '__main__':
def produce_from_last_line_of_file(topic=str, filepath=str, parsed_files_filepath=str):   
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default_producer'])
    
    config_port = str(config['bootstrap.servers'])
    
    
    # topic = 'raw-events'
    
    # # Check if the topic has no messages 
    
    # consumer_config = config
    # consumer_config.update(config_parser['consumer'])
    # consumer = Consumer(config)
    # consumer.subscribe([topic])
    # # Poll for messages with a short timeout
    # msg = consumer.poll(timeout=1.0)
    # if msg is None:
    #     print(f'The topic {topic} has no messages')    
    # consumer.close()    
    
    # msg = 0
    # # If topic does not exist or has no messages, delete the contents 
    # if topic not in all_topics_list or msg is None:
    #     print('Deleting the parsed files contents...\n')
    #     # Truncate the file to start writing new events to it
    #     with open(parsed_files_filepath, 'w'):
    #         pass
              
    
    # # If the topic does not exist, create it and delete the parsed files file
    # if assert_topic_and_create(topic, config_port):
    #     print('Deleting the parsed files contents...\n')
    #     # Truncate the file to start writing new events to it
    #     with open(parsed_files_filepath, 'w'):
    #         pass
    
        

    create_topic(topic, config_port)
            
    # # Get the compressed file filepath
    # folder_path = '/home2/output/2024-04-01'
    # filename = '2024-04-01-1.json.gz'
    # filepath = os.path.join(folder_path, filename)

    # filepath = '/home2/other_data/2015-01-01-15-thinned.json.gz'
    # Produce from file into topic
    the_whole_file_was_read, the_whole_file_was_read_beforehand = produce_from_line_we_left_off(topic, filepath, parsed_files_filepath, config)
    return the_whole_file_was_read, the_whole_file_was_read_beforehand
    
    
# # Demo
# The path to the file containing the state of the lines read from each file
# parsed_files_filepath = "/home2/output/files_parsed.json"

# # Choose and uncomment one of the paths below:
# # Path with many events
# filepath = '/home/xeon/Thesis/local-kafka-flink-cassandra-integration/presentation-10-demo/task-1-preprocess-download-and-thin-data/og_vs_thinned_events/2024-01-01-0-thinned-oneliner.json.gz'

# # Path with custom events
# filepath = '/home/xeon/Thesis/local-kafka-flink-cassandra-integration/presentation-10-demo/task-1-preprocess-download-and-thin-data/og_vs_thinned_events/2024-01-01-0-thinned-oneliner-custom-events.json.gz'
# produce_from_last_line_of_file(filepath, parsed_files_filepath)