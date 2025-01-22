'''
Gets a .json.gz file from folder input, thins it into output, if it does not
exist already and produces events from the input folder
'''
import os, time, json, gzip
from datetime import datetime

from produce_from_last_line_of_file import produce_from_last_line_of_file,\
    create_topic, delete_topic
from argparse import ArgumentParser, FileType
from configparser import ConfigParser


from config_topic_from_outside_the_container import \
    purge_topic_via_configuration, restore_default_configs_of_topic, \
    is_topic_empty, purge_topic_via_command

from check_job_status import check_if_job_is_busy

from consume_and_store_repos_from_repos_list_offsets_non_verbose import \
    consume_events_and_store_repos_in_table, insert_repo_in_table_add_all_values, \
    consume_events_and_store_repos_in_table_concurrently

from consume_and_store_stats_from_stats import consume_events_and_store_stats_in_table, \
    consume_events_and_store_stats_in_table_concurrently

def date_2_string(datetime_to_convert):
    return datetime.strftime(datetime_to_convert, '%Y-%m-%d-%H')

def string_2_date(string_to_convert):
    return datetime.strptime(string_to_convert, '%Y-%m-%d-%H')




def get_the_next_hours_to_produce_from(parsed_files_filepath, \
    folderpath_to_produce_from, \
    number_of_files_to_produce_in_batch = 1):
    
    """
    Given the files_parsed.json file and the 
    ``number_of_files_to_produce_from_in_batch`` returns a list with the hours
    of the files to produce from into topic 'raw-events'
    """
    hours_to_produce_from = []
    
    if os.path.exists(parsed_files_filepath):
        # If the parsed_files file has no contents, return the first file
        if os.stat(parsed_files_filepath).st_size == 0:
            hour_to_start_producing_from = 0
        else:
            with open(parsed_files_filepath, 'r') as parsed_files:
                dict_of_parsed_files = json.load(parsed_files)
            
            parsed_files_names_list = list(dict_of_parsed_files.keys())
            
            # Start producing from the file in the list
            # that was not produced from to its entirety
            
            last_file_in_parsed_files = parsed_files_names_list[-1]

            filepath_to_produce_from = \
                os.path.join(folderpath_to_produce_from, last_file_in_parsed_files)
            
            # Calculate the number of lines in the gharchive file
            with gzip.open(filepath_to_produce_from, 'r') as file_object:
                total_lines_in_last_file = len(file_object.readlines())
            
            # If not all lines in the file have been produced, start from 
            # this file. Example: If 2 has not been entirely produced
            # and number_of_files_to_produce_from = 3
            # start from hour 2 up to 4 [2, 3, 4]
            if dict_of_parsed_files[last_file_in_parsed_files] \
                < total_lines_in_last_file:
                    hour_of_file = last_file_in_parsed_files.rsplit('-', 1)[1] 
                    hour_to_start_producing_from = int(hour_of_file.replace('.json.gz', ''))
            # If the entire last file was read, start from the next one
            elif dict_of_parsed_files[last_file_in_parsed_files] \
                == total_lines_in_last_file:
                    hour_of_file = last_file_in_parsed_files.rsplit('-', 1)[1] 
                    hour_to_start_producing_from = int(hour_of_file.replace('.json.gz', '')) + 1
    else:
        raise ValueError(f"Path {parsed_files_filepath} does not exist")
            
            
    for i in range(number_of_files_to_produce_in_batch):
        # The hours we produce from range from 0 to 23
        if hour_to_start_producing_from + i <= 23:
            hours_to_produce_from.append(hour_to_start_producing_from + i)
    return hours_to_produce_from



def get_the_next_hours_to_produce_from_with_datetime(parsed_files_filepath, \
    folderpath_to_produce_from, number_of_files_in_batch):
    '''
    Gets the last hour of the file 
    '''
    next_hours_list = []
    
    # Get the day to start producing from
    day_to_start_producing_from = os.path.basename(folderpath_to_produce_from) 
    
    # Get the hour of the day to start from 
    if os.stat(parsed_files_filepath).st_size == 0:
            hour_to_start_producing_from = 0
    else:
        with open(parsed_files_filepath, 'r') as parsed_files:
            dict_of_parsed_files = json.load(parsed_files)
        
        # Get the hours in a list
        hours_produced_from_all_dates = [file.replace('.json.gz', '') \
            for file in dict_of_parsed_files.keys()]
        
        # print(hours_produced_from_all_dates)
        # exit()
        
        hours_produced_from_folderpath_date = []
        for hour_of_date in hours_produced_from_all_dates:
            day_of_hour = date_2_string(string_2_date(hour_of_date)).rsplit('-', 1)[0]
            if  day_of_hour  == \
                os.path.basename(folderpath_to_produce_from):
                
                hours_produced_from_folderpath_date.append(hour_of_date)
        
        # print(hours_produced_from_folderpath_date)
        # exit()
        
        # Get the last hour of the day
        # If there are no hours produced from the day
        if hours_produced_from_folderpath_date != []:
            date_to_start_producing_from = max([string_2_date(hour) \
                for hour in hours_produced_from_folderpath_date])
            hour_to_start_producing_from = date_to_start_producing_from.hour
        else:
            date_to_start_producing_from = datetime.strftime(\
                datetime.strptime(day_to_start_producing_from, "%Y-%m-%d"), "%Y-%m-%d") 
            hour_to_start_producing_from = 0

    for i in range(number_of_files_in_batch):
        # The hours we produce from range from 0 to 23
        if hour_to_start_producing_from + i <= 23:
            next_hours_list.append(hour_to_start_producing_from + i)
    
    
    # If the last file of the folder is read entirely, return an empty list
    if next_hours_list == [23]:
        filepath_to_produce_from = \
                os.path.join(folderpath_to_produce_from, \
                    os.path.basename(folderpath_to_produce_from) + '-23.json.gz')
        # Calculate the number of lines in the gharchive file
        with gzip.open(filepath_to_produce_from, 'r') as file_object:
            total_lines_in_last_file = len(file_object.readlines())
        
        
        last_filename = os.path.basename(folderpath_to_produce_from) + '-23.json.gz'
        if dict_of_parsed_files[last_filename] \
                == total_lines_in_last_file:
            next_hours_list = []
    
    return next_hours_list


# # 1.2 Produce events from given folder
def produce_from_folder(folderpath_to_produce_from, parsed_files_filepath, topic, \
    number_of_files_to_produce_in_batch=1):
    
    """
    Description:
    Produces events from the gharchive files of a folder
    
    Arguments:
    ``folderpath_to_produce_from``: The folder containing the files
    ``parsed_files_filepath``: The filepath to the parsed_files.json containing the files
    with the events and the event lines that were already produced
    ``topic``: The topic to produce the raw events into
    ``number_of_files_to_produce_in_batch``: The number of files from the folder to 
    produce events from
    
    Output:
    ``sections_performances``: A list of lists as so: 
    [[section_name_0, time_elapsed_for_section_0], ..., 
            [section_name_n, time_elapsed_for_section_n]]
            
    """
    sections_performance = []
    
    total_dur = 0
    
    # Section 1: Parse args, get hours, create topic
    st = time.time()
    # # Get the bootstrap-server:port from the configuration file
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default_producer'])
    config_server_and_port = str(config['bootstrap.servers'])
    
    # Produce events from each file in the output file with the thinned events    
    
    hours_to_produce_from = get_the_next_hours_to_produce_from_with_datetime(\
        parsed_files_filepath, folderpath_to_produce_from,\
        number_of_files_to_produce_in_batch)
    
    
    # If the entire folder was read, stop execution
    if hours_to_produce_from == []:
        print(f"The entire folder {folderpath_to_produce_from} has already been read.\n"
            f"Choose another folder to produce from")
        exit()
    
    
    # Create topic if it does not exist
    create_topic(topic, config_server_and_port)
    
    
    et = time.time()
    dur = et - st
    total_dur += dur
    sections_performance.append(["1. Parse args, get hours, create topic", dur])
    
    # Section 2: Produce events
    st = time.time()
    # Produce messages from number_of_files_to_produce_in_batch 
    for file_hour in hours_to_produce_from:
        
        filepath_to_produce_from = folderpath_to_produce_from + '/' + \
            os.path.basename(folderpath_to_produce_from) + '-' + str(file_hour) + '.json.gz'
        the_whole_file_was_read = produce_from_last_line_of_file(topic, filepath_to_produce_from, \
            parsed_files_filepath)
        
        # # If the file was not read, stop execution (do not consume data 
        # # until the whole file was read)
        # if not the_whole_file_was_read:
        #     raise ValueError("The entire file was not read. Transformation of data may not begin")    
    
    et = time.time()
    dur = et - st
    total_dur += dur
    sections_performance.append(["2. Produce events", dur])

    
    
    # Section 3: Transform datastream into repos and stats
    st = time.time()
    # When producer stops (either from keyboard interrupt, or
    # from EOF), start transformation of messages in topic "raw-events"
    # Transoformation should already be running in the background 
    # as the flink job is running indefinitely
    
    # On EOF (the_whole_file_was_read==True, 
    # check if the transformation is still ongoing
    if not the_whole_file_was_read:
        # Wait until datastream transformation operators start working
        while(check_if_job_is_busy() == False):
            print("Waiting until datastream transformation operators start working")
            time.sleep(5)
        
        # Wait until transform stops
        while(check_if_job_is_busy() == True):
            print("Waiting for transformation of data to stop")
            time.sleep(5)

        print(f'Derived all repos and stats for hour(s) {hours_to_produce_from} from raw-events')
    else:
        # Wait until transform stops
        while(check_if_job_is_busy() == True):
            print("Waiting for transformation of data to stop")
            time.sleep(10)
        print(f'Derived all repos and stats for hour(s) {hours_to_produce_from} from raw-events')
    
    et = time.time()
    dur = et - st
    total_dur += dur
    sections_performance.append(["3. Transform datastream into repos and stats", dur])
    
    
    # Section 4: Repos and stats produced in Cassandra
    st = time.time()
    # When transformation of raw-events data ends (meaning repos and 
    # stats are full from one day), start consuming topics "repos" and "stats" 
    # from oldest to newest offset as stats are aggregated as time goes by 
    # into Cassandra meaning new events overwrite old ones
    print(f"Consuming repos of the hour(s) {hours_to_produce_from}")
    # consume_events_and_store_repos_in_table()
    
    consume_events_and_store_repos_in_table_concurrently()
    
    print(f"Consuming stats of the hour(s) {hours_to_produce_from}")
    # consume_events_and_store_stats_in_table()
    
    consume_events_and_store_stats_in_table_concurrently()
    
    et = time.time()
    dur = et - st
    total_dur += dur
    sections_performance.append(["4. Repos and stats produced in Cassandra", dur])
    
    
    # Section 5
    st = time.time()
    # Purge topics raw-events and repos, only if consumption of messages
    # has stoppped
    if the_whole_file_was_read:
        
        
        # Purge topic 'raw-events'
        json_folderpath = "/home/xeon/Thesis/local-kafka-flink-cassandra-integration/presentation-10-demo/task-2-store-tables-repos-and-stats/"
        print(f"Deleting messages of topic 'raw-events'...")
        purge_topic_via_command(topic, config_server_and_port, \
            json_folderpath)
        
        
        # Purge topic 'repos' after every hour
        print("Deleting messages of topic 'repos'...")
        purge_topic_via_command('repos', config_server_and_port, \
            json_folderpath)
        
        # Delete topic instead of waiting to purge it
        delete_topic(topic, config_server_and_port)
    
    # If the last file of the day was used, purge the stats topic
    if hours_to_produce_from[-1] == 23:
        
        # Purge topic 'stats' after events from all hours of a day were produced
        print("Deleting messages of topic 'stats'...")
        purge_topic_via_command('stats', config_server_and_port, \
            json_folderpath)
        
        
    
    et = time.time()
    dur = et - st
    total_dur += dur
    sections_performance.append([f"5. Topic {topic} deleted", dur])
    
    sections_performance.append(["Total time elapsed", total_dur])
    return sections_performance

# Once ALL the files in the folder of the day are read and consumed into
# Cassandra, delete the topic stats manually

    
    

# Demo

# Directory with the thinned events
out_dir_path = '/home2/output/2024-01-02'
# The path to the file containing the state of the lines read from each file
parsed_files_filepath = "/home2/output/files_parsed.json"
topic = 'raw-events'
sections_performance = produce_from_folder\
    (out_dir_path, parsed_files_filepath, topic,\
    number_of_files_to_produce_in_batch=1)


print(f"Execution times in seconds: {sections_performance}")