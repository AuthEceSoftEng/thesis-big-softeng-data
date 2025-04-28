from produce_from_last_line_of_file import extract_compressed_file_from_path
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, admin


from thin_data import thin_data_of_file
from heavy_thin_of_data import heavy_thin_data_of_file
import time, sys

import json
import subprocess
from datetime import datetime, timedelta
import requests
import os

import gzip
from delete_and_recreate_topic import get_kafka_broker_config, get_topic_number_of_messages, create_topic_if_not_exists, delete_topic_if_full


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
        r = requests.get(gha_file_url, stream=True)
        print('Starting download of GHArchive file from URL: {}'.format(gha_file_url))
        with open(filepath, 'wb') as f:
            for chunk in r.raw.stream(1024, decode_content=False):
                if chunk:
                    f.write(chunk)
        print("GHArchive file from URL {} finished downloading".format(gha_file_url))
    else:
        print(f"File {filename} already exists in folder {folderpath}.")


def get_subtasks_endpoints_of_job_in_flink(job_name, hostname):
    r"""
    Description
    Returns a list with the URL endpoints of the subtasks of a
    running flink job's tasks (the job is identified using its name declared 
    in the env.execute(job_name)).
    
    :param job_name: The name of the job whose subtasks of tasks we need
    
    :returns subtask_of_task_endpoint_list: A list with the URL endpoints of the subtasks of a
    flink job's tasks.
    
    :returns None: if the job does not exist (either no jobs are running or no job with this name 
    is running)
    
    Assumptions/Prerequisites: 
    - Port of localhost where the Flink web UI is deployed is locahost:8081
    - Only a single job with the name ``job_name`` is running at a time in the Flink cluster
    - Responses of endpoints are according to the Flink REST API:
    https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/rest_api/#api
    
    Example: returns [[0, 0, 0, 1], [0.3, 0.3, 0.1, 0.9], [0.5, 0.4, 0.2, 0.8]]
    which means that the pyflink job contains 3 tasks with 4 subtasks each
    with each one having the aforementioned busyRatios
    """
    # For secure connection:
    # Setup SSL for the Flink REST API: 
    # see: https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/security/security-ssl/#configuring-ssl
    # For for local use and for simplicity reasons no ssl is used.

    
    # Check if the jobs endpoint exists
    all_jobs_ids_url = f"http://{hostname}:8081/jobs/"
    res = requests.get(all_jobs_ids_url)
    if res.status_code != 200:
        raise ValueError(f"Endpoint {all_jobs_ids_url} could not be accessed\n"
                        f"HTTP status code: {res.status_code}")

    
    # If the job has the name of that job in the execute, we keep its id and return its subtasks normally.

    # The jobs response of the endpoint in JSON format
    jobs_json_dict = res.json()["jobs"]

    # Get the jobs' IDs with status "RUNNING"
    jobs_id_list = [job_element["id"] for job_element in jobs_json_dict if job_element["status"]=="RUNNING"]

    # If no jobs are running return None
    if jobs_id_list == []:
        return None

    # If there are running jobs,
    # Initialize the job's id as None
    id_of_the_wanted_job = None

    # Find the ID of the job given its name    
    for job_id in jobs_id_list:
        running_job_id = f"http://{hostname}:8081/jobs/" + job_id
        res = requests.get(running_job_id)
        response_job_id = res.json()["plan"]["name"]
        if response_job_id == job_name:
            id_of_the_wanted_job = job_id
            
            # Assumption is that only one running job has this name
            # meaning there are not more than one jobs running simultaneously 
            # with the same name
            break

    # If there is no running job with this name, return None
    if id_of_the_wanted_job == None:
        return None
    
    # # Print JobIDs
    # print(jobs_id_list)

   
    # Extract all vertices meaning all tasks of the single job we found beforehand
    vertices_url = f"http://{hostname}:8081/jobs/" + id_of_the_wanted_job
    res = requests.get(vertices_url)
    if res.status_code != 200:
        raise ValueError(f"Endpoint {vertices_url} could not be accessed\n"
                        f"HTTP status code: {res.status_code}")

    vertices_json_dict = res.json()["vertices"]
    vertices_ids_list = [vertex["id"] for vertex in vertices_json_dict]


    # Get subtasks endpoints for the task (vertex)
    subtask_of_task_endpoint_list = []
    for vertex_id in vertices_ids_list:
        subtask_busy_endpoint = vertices_url + "/" + f"vertices/{vertex_id}" + "/backpressure"
        subtask_of_task_endpoint_list.append(subtask_busy_endpoint)
    
    return subtask_of_task_endpoint_list


def check_if_subtasks_of_task_are_busy(task_endpoint=str, \
    show_endpoint=False, verbose=True):
    '''
    Checks if the subtasks retrieved from the task_endpoint are busy
    
    ``task_endpoint``: HTTP endpoint to call to retrieve the subtasks 
    of all tasks in the pyflink job
    ``show_endpoint``: prints the endpoint for the task 
    ``verbose``: prints the subtasks busyRatios
    
    returns: True if the task is busy (meaning its subtasks are busy) 
    and
    False if it is not (meaning all its subtasks are idle/not busy)
    '''
    
    try:
        res = requests.get(task_endpoint)
        # On first call to the endpoint '/jobs/:jobid/vertices/:vertexid/backpressure', the jobs are not returned.
        # So, the endpoint is called again
        while res.json()["status"] == 'deprecated':
            print("Waiting for the jobs in the response")
            time.sleep(4)
            res = requests.get(task_endpoint)
        subtasks_of_task_list = res.json()["subtasks"]
    except KeyError as e:
        res = requests.get(task_endpoint)
        print("Error log:")
        print(f"Task endpoint: {task_endpoint}")
        print(f"Res.json(): {res.json()}")
        print()
        subtasks_of_task_list = res.json()["subtasks"]
    except Exception as e:
        print("Exception: ", e)
        
    subtasks_busy_ratios = [subtask["busyRatio"] for subtask in subtasks_of_task_list]
    
    # Print endpoint
    if show_endpoint==True:    
        print(f"For endpoint: {task_endpoint}:")
        
    if verbose==True:
        print(f"Subtasks' busy ratios: {subtasks_busy_ratios}")
        # Print message if ratio 
        if all(busy_ratio == 0 for busy_ratio in subtasks_busy_ratios):
            print("Task is idle (does not contain busy subtasks)")
        else:
            print("Task is busy (contains busy subtasks)")
        print("")
    
    if all(busy_ratio == 0 for busy_ratio in subtasks_busy_ratios):
        return False
    else:
        return True

def check_if_job_is_busy(job_name, hostname, show_endpoint=False, verbose=False):
    r"""
    Description
    Checks if the Flink job with name ``job_name`` is busy. 
    If the job exists and is busy, returns True
    If the job exists and is not busy, returns False
    If the job does not exist (is not running), returns None
    
    Assumption (see function 'get_subtasks_endpoints_of_job_in_flink'):
    The job is busy if there is at least one subtask of one task of the job that 
    has busyRatio != 0 (is busy) 
    """
    
    # Get the subtasks endpoints for all tasks in the flink job
    subtask_of_task_endpoint_list = get_subtasks_endpoints_of_job_in_flink(job_name, hostname) 
    
    # If a running job with this name does not exist, return None
    if subtask_of_task_endpoint_list == None:
        return None
    
    # If one task of the job is busy return True
    for subtask_of_task_endpoint in subtask_of_task_endpoint_list:
        if check_if_subtasks_of_task_are_busy(subtask_of_task_endpoint,\
            show_endpoint, verbose) == True:
            return True
    # If no task is busy return False
    return False

def get_job_busy_ratio(job_name, hostname):
        """
        hostname: localhost or the docker container name for the host
        """
        # Negative value 
        max_subtask_busy_ratio = -1
        
        # Example 'subtasks_endpoints' value for a single task: ['http://localhost:8081/jobs/01b057650c7c38e477093b244a85a5e3/vertices/587fdcdfae7cef1ad77583e805baf432/backpressure']
        tasks_endpoints_list = get_subtasks_endpoints_of_job_in_flink(job_name, hostname) 
        
        for task_endpoint in tasks_endpoints_list:
            task_info_res = requests.get(task_endpoint)    
            # Example 'subtasks' value: [{'subtask': 0, 'backpressureLevel': 'ok', 'ratio': 0.0, 'idleRatio': 1.0, 'busyRatio': 0.0, 'backpressure-level': 'ok'}, {'subtask': 1, 'backpressureLevel': 'ok', 'ratio': 0.0, 'idleRatio': 1.0, 'busyRatio': 0.0, 'backpressure-level': 'ok'}]
            try:
                subtasks_of_task = task_info_res.json()["subtasks"]
            except KeyError as e:
                raise KeyError(f"KeyError: {e}. Original json fields: {subtasks_of_task}")
            subtasks_busy_ratios = [subtask["busyRatio"] for subtask in subtasks_of_task]
            # The busy ratio of a job is the largest 'busy ratio' value of 
            # all the job's tasks' subtasks
            for subtask_busy_ratio in subtasks_busy_ratios:
                if subtask_busy_ratio > max_subtask_busy_ratio:
                    max_subtask_busy_ratio = subtask_busy_ratio
        
        return max_subtask_busy_ratio
    
def get_running_job_names(hostname):
    """
    Returns a list with the names of the running pyflink jobs
    If no pyflink job is running, returns an empty list ([])
    """
    jobs_endpoint = f"http://{hostname}:8081/jobs/"
    try:
        jobs_res = requests.get(jobs_endpoint, timeout=5)
    except Exception as e:
        # # Original exception
        print("Original exception: ", e)
        print()
        raise Exception("Tried accessing the flink jobs deployed in the cluster. The Flink cluster is not running. Deploy the Flink cluster and rerun the script.")
    
    job_objects = jobs_res.json()["jobs"]
    running_jobs_ids = [job_object["id"] for job_object in job_objects if job_object["status"] == "RUNNING"]
    
    running_job_names = []
    for running_job_id in running_jobs_ids:
        job_info_endpoint = jobs_endpoint + f"{running_job_id}"
        job_name_res = requests.get(job_info_endpoint)
        job_name = job_name_res.json()["name"]
        running_job_names.append(job_name)
    
    return running_job_names
    
 

# Samples GHArchive file to get only its first k lines into another file 
# for testing the produce-into-kafka-topic process
def create_file_with_k_first_lines(input_filepath, output_filepath, number_of_lines_to_keep, do_again=False):
    '''
    Sapmles the k first events of the gharchive filepath `input_filepath` 
    and stores it into `output_filepath`.
    If the do_again is True, the decompression is done again and 
    the existing file is overwritten
 
    `input_filepath`: GHArchive file with thinned events
    `output_filepath`: GhArchive file with the first ``number_of_lines_to_keep`` 
    first  events 
    `do_again`: In case the GHArchuive file with the thinned events
    already exists, if do_again is set to True, 
    thin the events again and overwrite the output_filepath contents
    '''
    # Calculate size of file (number of lines of file)
    with gzip.open(input_filepath, 'r') as file_object:
            lines_in_file = len(file_object.readlines())
    
    number_of_lines_thinned_per_print = 1000

    print(f'Dumping {number_of_lines_to_keep} events of {os.path.basename(input_filepath)} into {os.path.basename(output_filepath)}...')
    if not os.path.exists(output_filepath) or do_again == True:
        with gzip.open(output_filepath, 'wb') as outstream:
            with gzip.open(input_filepath) as instream:
                for i, line in enumerate(instream):
                    # Show lines produced
                    if i % number_of_lines_thinned_per_print == 0:
                        sys.stdout.write("\r JSON events dumped: {0}/{1}".format(i, lines_in_file))
                        sys.stdout.flush()				
                    if i == number_of_lines_to_keep:	
                            break	
                    outstream.write(line)
                # When done producing all the lines, print all lines ingested 
                # in stdout
                sys.stdout.write("\r JSON events dumped: {0}/{1}".format(number_of_lines_to_keep, lines_in_file))
                sys.stdout.flush()
                        
    elif os.path.exists(output_filepath) and do_again == False:
        print(f"{os.path.basename(output_filepath)} with the first {number_of_lines_to_keep} events already exists.")
    print('...Done')


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

def produce_all_lines_of_file(topic=str, filepath=str, config=dict):
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
    decompressed_file_path = extract_compressed_file_from_path(\
        filepath)
    with open(decompressed_file_path, 'r') as file_object:
        lines_in_file = len(file_object.readlines())
    line_we_left_off = 0    
    lines_produced = 0
    number_of_lines_produced_per_print = 1000	
    time_between_produced_messages = pow(10, -8)
    number_of_messages_before_poll = 100
    
    
    try:
        producer = Producer(config)
        with open(decompressed_file_path, 'r') as file_object:
            
            lines = file_object.readlines()
            print(f"Reading lines of {filename} until EOF or keyboard interrupt...")
            print(f'Producing events from line No.{line_we_left_off+1} of {filename}')                
            for i in range(line_we_left_off, lines_in_file):    
                jsonDict = json.loads(lines[i])
                jsonStr = str(jsonDict)

                if lines_produced % number_of_lines_produced_per_print == 0:
                    sys.stdout.write("\rJSON objects produced: {0}/{1}".format(lines_produced, lines_in_file))
                    sys.stdout.flush()
                
                producer.produce(topic, value= jsonStr, callback=delivery_callback)
                # producer.produce(topic, value=lines[i], callback=delivery_callback)
                
                lines_produced = lines_produced+1
                
                # Poll to cleanup the producer queue after every message production
                producer.poll(0)
                
                # Optional: Uncomment to stop producing messages 
                # after a number of them
                if lines_produced == 1000: 
                    break
                
                # Short time to capture output
                time.sleep(time_between_produced_messages)
            
            # Once the total number of lines were produced, print it
            sys.stdout.write("\rJSON objects produced: {0}/{1}".format(i+1, lines_in_file))
            sys.stdout.flush()
    
    except KeyboardInterrupt:
        print('\nKeyboard interrupt\n')
    finally:
        if lines_produced == lines_in_file:
            print('\nEOF\n')
        # Delete decompressed .json file after messages were produced
        if os.path.exists(decompressed_file_path):
            os.remove(decompressed_file_path)
        
        # Wait for messages' delivery
        producer.poll(10000)
        producer.flush()                
        print('\nProducer closed properly')
    
    



if __name__ == '__main__':
    # Code sketch
    
    starting_date_formatted = '2024-12-03-14'
    ending_date_formatted = '2024-12-03-14'
    current_date_formatted = starting_date_formatted
    starting_date = datetime.strptime(starting_date_formatted, '%Y-%m-%d-%H')
    ending_date = datetime.strptime(ending_date_formatted, '%Y-%m-%d-%H')
    current_date = datetime.strptime(current_date_formatted, '%Y-%m-%d-%H')
    
    
    sections_performance = []
    total_dur = 0

    while current_date <= ending_date:
        skip_steps_1_3 = False
        if skip_steps_1_3 == False:
                
            # 1. Download gharchive file if not exists
            # region
            
            st = time.time()
            
            gharchive_file_URL = 'https://data.gharchive.org/' + current_date_formatted + '.json.gz'
            folderpath_to_download_into = '/github_data_for_speed_testing'
    
            # Thinned file name and path
            filename = current_date_formatted + '.json.gz'
            filepath_to_download_into = os.path.join(folderpath_to_download_into, filename)
            filepath_of_file_to_thin = filepath_to_download_into
            # Thinned output
            folderpath_of_thinned_file = folderpath_to_download_into
            thinned_filename = current_date_formatted + '-thinned.json.gz'
            filepath_of_thinned_file = os.path.join(folderpath_to_download_into, thinned_filename)
    
            # If neither the original or the thinned file exist, download the original to produce it
            if not os.path.exists(filepath_of_file_to_thin) and not os.path.exists(filepath_of_thinned_file):
                download_compressed_GHA_file(gharchive_file_URL, folderpath_to_download_into)

            et = time.time()
            dur = et - st
            total_dur += dur
            sections_performance.append(["1. Download gharchive file", dur])

            #endregion 

            # 2. Thin the gharchive file (filename -> filename-thinned.json.gz)
            # region
            st = time.time()

            
            heavy_thin_data_of_file(filepath_of_file_to_thin, filepath_of_thinned_file)

            input_filepath = f'/github_data_for_speed_testing/{current_date_formatted}-thinned.json.gz'
            number_of_lines_to_keep = 200000
            limited_number_of_lines_filepath = f'/github_data_for_speed_testing/{current_date_formatted}-thinned_first_{number_of_lines_to_keep}_only.json.gz'
            create_file_with_k_first_lines(input_filepath, limited_number_of_lines_filepath, number_of_lines_to_keep)
            
            
            et = time.time()
            dur = et - st
            total_dur += dur
            sections_performance.append(["2. Thin file", dur])

            # endregion
            
            # 3. Produce all lines of the sampled thinned file into the topic
            # region
            st = time.time()

            topic_to_produce_into = 'historical-raw-events'
            parsed_files_filepath = "/github_data_for_speed_testing/files_parsed.json"
            
            # Get kafka_host:kafka_port
            parser = ArgumentParser()
            parser.add_argument('config_file', type=FileType('r'))
            args = parser.parse_args()
            config_parser = ConfigParser()
            config_parser.read_file(args.config_file)
            config = dict(config_parser['default_producer'])
            config_port = str(config['bootstrap.servers'])
                
            create_topic_if_not_exists(topic_to_produce_into, config_port)
            
            
            
            produce_all_lines_of_file(topic_to_produce_into, limited_number_of_lines_filepath, config)
        


            et = time.time()
            dur = et - st
            total_dur += dur
            sections_performance.append(["3. Produce thinned events", dur])

            # endregion

            
        skip_step_4 = True
        if skip_step_4 == False:
            # 4. Wait for data transformation (Check if the jobs stopped working)
            # region
            st = time.time()

            hostname = 'jobmanager'
            # hostname = 'taskmanager'
            # hostname = 'localhost'
            
            running_job_names_in_cluster = get_running_job_names(hostname)
                
            
            if running_job_names_in_cluster == []:
                raise Exception("No jobs are running on the Flink cluster. Execute a job and rerun the producer")
            
            
            
            is_a_job_running = None
            for single_job_name in running_job_names_in_cluster:
                is_a_job_running = check_if_job_is_busy(single_job_name, hostname)
                if is_a_job_running == True:        
                    break
            if (is_a_job_running == False):    
                print(f"Pyflink jobs have not started working")
                print("Waiting for the jobs to start")
            while(is_a_job_running == False):

                for single_job_name in running_job_names_in_cluster:
                    # If one of the pyflink jobs started working, break the while loop 
                    is_a_job_running = is_a_job_running or check_if_job_is_busy(single_job_name, hostname)
                    job_busy_ratio = get_job_busy_ratio(single_job_name,  hostname)
                    sys.stdout.write(f"\rJob: '{single_job_name}', busy ratio {round(job_busy_ratio*100, 1)}%\n")
                sys.stdout.flush()
                if is_a_job_running == True:        
                    break
                time.sleep(5)
                sys.stdout.write("\033[F" * len(running_job_names_in_cluster)) 
                    
            
            print()
            print(f"Pyflink jobs have started working")
            print("Waiting for pyflink jobs to stop")
            while(is_a_job_running == True):
                # Reset the job status. 
                is_a_job_running = False
                for single_job_name in running_job_names_in_cluster:
                    #  While there is at least one working job, wait for it to finish
                    is_a_job_running = is_a_job_running or check_if_job_is_busy(single_job_name, hostname)
                    job_busy_ratio = get_job_busy_ratio(single_job_name, hostname)        
                    sys.stdout.write(f"\rJob: '{single_job_name}', busy ratio {round(job_busy_ratio*100, 1)}%\n")
                sys.stdout.flush()
                if is_a_job_running == False:        
                        break
                time.sleep(5)
                sys.stdout.write("\033[F" * len(running_job_names_in_cluster))  
            
            print()
            print("All pyflink jobs stopped working")
            
            et = time.time()
            dur = et - st
            total_dur += dur
            sections_performance.append(["4. Wait for flink jobs to finish", dur])
            # endregion

        current_date = current_date + timedelta(hours=1)
        current_date_formatted = datetime.strftime(current_date, '%Y-%m-%d-%-H')
        

        skip_topic_deletion = True
        if skip_topic_deletion == False:
            # Delete and recreate the topic if too large
            topic = topic_to_produce_into
            bootstrap_servers = get_kafka_broker_config(topic)
            number_of_messages = get_topic_number_of_messages(topic, bootstrap_servers)
            max_number_of_messages = 2000000
            delete_topic_if_full(topic, max_number_of_messages, bootstrap_servers)
            # Short delay to update kafka cluster metadata before recreating the topic
            time.sleep(5)
            create_topic_if_not_exists(topic, bootstrap_servers)
    
    sections_performance.append(["Total time elapsed", total_dur])
    print("Execution times in seconds:\n")
    for single_section_performance in sections_performance:
            print(f"{single_section_performance[0]}: {round(single_section_performance[1], 1)} sec")
