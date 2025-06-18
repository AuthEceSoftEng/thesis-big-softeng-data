# from produce_from_last_line_of_file import produce_from_last_line_of_file
from confluent_kafka import Producer, Consumer, admin, TopicPartition, KafkaException

from datetime import datetime, timedelta
import requests
import os
from argparse import ArgumentParser, FileType
from configparser import ConfigParser

from thin_data import thin_data_of_file

import time
import sys
import json
import gzip
import shutil
from pathlib import Path


def get_newest_GHArchive_file_URL():
    print("Retrieving the latest GHArchive file")
    # Get current time
    currDate = datetime.now() 
    tomorrowDate = currDate + timedelta(1)
    statusCode = None

    # Can be either currDate or tomorrowDate
    startingDate = currDate

    # Initialise values for the first iteration of the while loop
    hoursOffset = -1
    dateByHourFormatted = startingDate.strftime('%Y-%m-%d-%H')
    
    # Get URL and status code from request
    url = "https://data.gharchive.org/{}.json.gz".format(dateByHourFormatted)
    # Get only the headers without downloading the file
    myRequest = requests.head(url) 
    statusCode = myRequest.status_code


    while statusCode != 200:
        hoursOffset += 1
        # Starting from tomorrow go back hour by hour to get the newest file
        # Note: We go back from tomorrow in case the program is used by someone 24 hours 
        # behind the time when the last file is uploaded
        dateByHour = startingDate - timedelta(hours=hoursOffset)
        dateByHourFormatted = dateByHour.strftime('%Y-%m-%d-%H')
        print("File for date {} does not exist: Status code {}".format(dateByHourFormatted, statusCode))
    
        # Get URL and status code from request
        url = "https://data.gharchive.org/{}.json.gz".format(dateByHourFormatted)
        # Get only the headers without downloading the file
        myRequest = requests.head(url) 
        statusCode = myRequest.status_code

        
    url = "https://data.gharchive.org/{}.json.gz".format(dateByHourFormatted) 
    print("Newest file available that exists is for date {}: Status code {}".format(dateByHourFormatted, statusCode))
    print("GHArchive file retrieved")
    newestGHAFileDate = dateByHourFormatted
    return url, newestGHAFileDate

def download_compressed_GHA_file(gha_file_url, folderpath):
    '''
    Downloads the compressed gharchive file from ``gha_file_url`` into ``folderpath``
    
    ``gha_file_url``: The URL of the gharchive file to download
    ``folderpath``: The folderpath to download the gharchive file into
    '''
    
    filename = gha_file_url.replace('https://data.gharchive.org/', '')
    filename = filename.replace('.json.gz', '')
    
    filepath = os.path.join(folderpath, filename) + '.json.gz'
    
    # Stream the contents of the file from the newest_GHArchive_file_url in chunks
    # and save it locally in "path_to_download_GHAFile"
    if not os.path.exists(filepath):
        
        with requests.get(gha_file_url, stream=True) as r:
            r.raise_for_status()
            r.raw.decode_content = False
            
            print('Starting download of GHArchive file from URL: {}'.format(gha_file_url))
            
            
            with open(filepath, 'wb') as f:
                for chunk in r.raw.stream(1024, decode_content=False):
                    if chunk:
                        f.write(chunk)
                    else:
                       break 
                        
                # for chunk in iter(lambda: r.raw.read(1024), b''):
                #     f.write(chunk)
                
        print("GHArchive file from URL {} finished downloading".format(gha_file_url))
    else:
        print(f"File {filename} already exists in folder {folderpath}.")


def save_files_parsed(files_parsed=dict, filepath_to_store_files_parsed=str):
    '''
    Save the state of the gharchive files parsed and are no longer needed 
    - ``files_parsed``: dictionary with the files_parsed. The filename is key and the
    value is the number of lines parsed from the file
    e.g. files_parsed = {'2024-04-01-0.json.gz':'end', '2024-04-01-1.json.gz':120}
    - ``filepath_to_store_files_parsed``: the filepath to store the 
    parsed files dictionary
    '''
    
    with open(filepath_to_store_files_parsed, "w") as new_state_file:
        # old_parsed_files_dict = restore_parsed_files(filepath_to_store_files_parsed)
        # old_parsed_file_line = old_parsed_files_dict[parsed_file]
        json.dump(files_parsed, new_state_file)
    
    
    # filename = os.path.basename(filepath_to_store_files_parsed)
    # print(f'Parsed lines of file {filename} were updated:')
    
    print(f'\nParsed lines of files were updated')
    
    # # # Uncomment the following section to print all the files parsed and 
    # # # the line we left off in each one
    # for parsed_file in files_parsed.keys():
    #     print(parsed_file, ':', files_parsed[parsed_file])
    # print()


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
        

def restore_parsed_files(parsed_files_filepath=str):
    '''
    Gets access to the files parsed to populate the Cassandra tables "repos" and "stats"
    ``parsed_files_filepath``: the filepath to the json file with the parsed files
    returns: ``files_parsed_dict`` which is a dict of the items in the parsed files
    '''
    if os.path.exists(parsed_files_filepath):
        with open(parsed_files_filepath, 'r') as parsed_files: 
            # If the parsed_files has no contents, print that the state is empty
            if  os.stat(parsed_files_filepath).st_size == 0:
                print("The producer has not parsed any files yet. Returning an empty dictionary")
                return {}
            else:
                dict_of_parsed_files = json.load(parsed_files)
    else: 
        raise ValueError('The state file for the parsed files does not exist. Cannot restore state')
    # files_parsed_dict = dict_of_parsed_files.items()
    # return files_parsed_dict
    return dict_of_parsed_files  


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

  
def produce_from_line_we_left_off(topic=str, filepath=str, \
    parsed_files_filepath=str, config=dict, topic_that_retains_message_order=str):
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
    
    number_of_lines_produced_per_print = 100
    seconds_between_produced_messages = pow(10, -6)
    number_of_messages_produced_between_delay = 100
    delay_between_message_batches = 10
    
    # Line tracker
    i = 0
    
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
                    
                    if i % number_of_lines_produced_per_print == 0:
                        sys.stdout.write("\rJSON objects produced: {0}/{1}".format(i+1, linesInFile))
                        sys.stdout.flush()
                    
                    
                    # JSON object to be inserted in the Cassandra database
                    jsonDict = json.loads(lines[i])
                    
                    jsonStr = str(jsonDict)
                    producer.produce(topic, value=jsonStr, callback=delivery_callback)
                    
                    jsonStrRetainingOrder = str({"id": jsonDict["id"], "created_at": jsonDict["created_at"]})
                    producer.produce(topic_that_retains_message_order, value=jsonStrRetainingOrder, callback=delivery_callback)
                    
                    linesProduced = i+1
                    
                    # # Break production if only 10000 events have been sent
                    # if i >= line_we_left_off + 20000:
                    #     break
                    
                    producer.poll(0)
                    
                    # Poll to cleanup the producer queue after every message production
                    # if i % 100 == 0: 
                    #     producer.poll(0)
                    # # Time sleep is used here to capture output
                    time.sleep(seconds_between_produced_messages)
                    
                    if i % number_of_messages_produced_between_delay == 0: 
                        time.sleep(delay_between_message_batches)
                        
                sys.stdout.write("\rJSON objects produced: {0}/{1}".format(i, linesInFile))
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


def create_topic_if_not_exists(topic, bootstrap_servers, desired_number_of_partitions):
    '''
    Checks if a topic exists and if it does not, it creates it
    `topic`: topic name 
    `config_port`: the port of the kafka cluster (e.g. localhost:44352)
    '''
    client = admin.AdminClient({"bootstrap.servers": bootstrap_servers})
    topic_metadata = client.list_topics(timeout=5)
    all_topics_list = topic_metadata.topics.keys()
    replication_factor = 1
                    
    # If the topic does not exist, create it    
    if topic not in all_topics_list:
        
        # Create topic
        print(f"Topic {topic} does not exist.\nCreating topic...")
        new_topic = admin.NewTopic(topic, num_partitions=desired_number_of_partitions, replication_factor=replication_factor)
        
        # Wait until topic is created
        try:
            create_topic_to_future_dict = client.create_topics([new_topic], operation_timeout=5)
            for create_topic_future in create_topic_to_future_dict.values():
                create_topic_future.result()
            
        # Handle create-topic exceptions
        except KafkaException as e:
            err_obj = e.args[0]
            err_name = err_obj.name()
            # If exists, create partitions
            if err_name == 'TOPIC_ALREADY_EXISTS':
                # Get current number of partitions
                print(f"Topic {topic} already exists and has {desired_number_of_partitions} partition(s)")
                topic_metadata = client.list_topics(timeout=5)
                # If partitions are few, increase them
                if current_number_of_partitions < desired_number_of_partitions:    
                    print(f"Increasing partitions of topic {topic} from {current_number_of_partitions} to {desired_number_of_partitions}...")
                    new_partitions = admin.NewPartitions(topic, new_total_count=desired_number_of_partitions)
                    # Wait until the number of the topic partitions is increased
                    create_partitions_futures = client.create_partitions([new_partitions])
                    for create_partition_future in create_partitions_futures.values():
                        create_partition_future.result()
                # Else, do nothing
                else:
                    print(f"Topic {topic} already exists and has {current_number_of_partitions} partitions")
                    pass
                    
            # Catch other errors
            else:
                raise Exception(e)
        print("Done")
    
    
    elif topic in all_topics_list:
        current_number_of_partitions = len(topic_metadata.topics[topic].partitions)
        if current_number_of_partitions != desired_number_of_partitions:
            new_partitions = admin.NewPartitions(topic, new_total_count=desired_number_of_partitions)
            # Wait until the number of the topic partitions is increased
            create_partitions_futures = client.create_partitions([new_partitions])
            for create_partition_future in create_partitions_futures.values():
                try:
                    create_partition_future.result()
                except KafkaException as e:
                    err_obj = e.args[0]
                    err_name = err_obj.name()
                    # If partitions are as many as they should be, do nothing
                    if err_name == 'INVALID_PARTITIONS':
                        print(f"Topic {topic} already exists and has {current_number_of_partitions} partitions")
    
    else:
        raise Exception(f"Topic {topic} neither exists or is absent from the kafka cluster")



    
    
if __name__=='__main__':
    
    # Download latest gharchive file
    newest_URL, newest_gharchive_file_date = get_newest_GHArchive_file_URL() 
    folderpath_to_download_into = '/github_data_real_time'
    download_compressed_GHA_file(newest_URL, folderpath_to_download_into)

    # Thin data
    # Input
    filename = newest_gharchive_file_date + '.json.gz'
    filepath_to_download_into = os.path.join(folderpath_to_download_into, filename)
    filepath_of_file_to_thin = filepath_to_download_into

    # Output
    folderpath_of_thinned_file = folderpath_to_download_into
    thinned_filename = newest_gharchive_file_date + '-thinned.json.gz'
    filepath_of_thinned_file = os.path.join(folderpath_to_download_into, thinned_filename)

    thin_data_of_file(filepath_of_file_to_thin, filepath_of_thinned_file)



    # Produce data from last file available:

    # # The path to the file containing the state of the lines read from each file
    parsed_files_filepath = "/github_data_real_time/files_parsed.json"
    topic_to_produce_into = 'real-time-raw-events'
    
    # The topic 'real-time-raw-events', thus retaining the message order
    topic_that_retains_message_order = 'real-time-raw-events-ordered'


    # region
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
        
        create_topic_if_not_exists(topic, config_port)
        
        # Produce from file into topic
        the_whole_file_was_read, the_whole_file_was_read_beforehand = produce_from_line_we_left_off(topic, filepath, parsed_files_filepath, config)
        return the_whole_file_was_read, the_whole_file_was_read_beforehand

    # Create topic 
    def get_bootstrap_servers():
        # Parse the command line.
        parser = ArgumentParser()
        parser.add_argument('config_file', type=FileType('r'))
        args = parser.parse_args()

        # Parse the configuration.
        # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        config_parser = ConfigParser()
        config_parser.read_file(args.config_file)
        config = dict(config_parser['default_producer'])
        
        bootstrap_servers = str(config['bootstrap.servers'])
        return bootstrap_servers
    
    bootstrap_servers = get_bootstrap_servers()
    create_topic_if_not_exists(topic_to_produce_into, bootstrap_servers, 4)

    create_topic_if_not_exists(topic_that_retains_message_order, bootstrap_servers, 1)

    # Produce from line we left off
    def get_default_producer_config():
         # Parse the command line.
        parser = ArgumentParser()
        parser.add_argument('config_file', type=FileType('r'))
        args = parser.parse_args()

        # Parse the configuration.
        # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        config_parser = ConfigParser()
        config_parser.read_file(args.config_file)
        default_producer_config = dict(config_parser['default_producer'])
        return default_producer_config
    
    default_producer_config = get_default_producer_config()    
    the_whole_file_was_read, _ = produce_from_line_we_left_off(topic_to_produce_into, filepath_of_thinned_file, parsed_files_filepath, default_producer_config, topic_that_retains_message_order)

    # the_whole_file_was_produced_already = produce_from_last_line_of_file(topic_to_produce_into, filepath_of_thinned_file, parsed_files_filepath)
    
    # endregion

