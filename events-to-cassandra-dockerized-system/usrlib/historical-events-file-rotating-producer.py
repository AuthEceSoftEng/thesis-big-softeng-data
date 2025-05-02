from produce_from_last_line_of_file import extract_compressed_file_from_path

from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, admin


from create_topic_from_inside_the_container import create_topic_from_within_container

from thin_data import thin_data_of_file
from heavy_thin_of_data import heavy_thin_data_of_file

import time, sys
# from check_job_status_multiple_jobs import check_if_job_is_busy 

# from check_job_status_multiple_jobs  import get_subtasks_endpoints_of_job_in_flink

import json
import subprocess
from datetime import datetime, timedelta
import requests
import os

from delete_and_recreate_topic import get_kafka_broker_config, get_topic_number_of_messages, create_topic_if_not_exists, delete_topic_if_full
from get_parsed_gharchive_files import save_files_parsed, restore_parsed_files


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

# Flink API functions to access job status
# region
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
    res = requests.get(task_endpoint)
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
        print(f"Res.json(): {res.json()}\n")
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
            subtasks_of_task = task_info_res.json()["subtasks"]
            subtasks_busy_ratios = [subtask["busyRatio"] for subtask in subtasks_of_task]
            # The busy ratio of a job is the largest 'busy ratio' value of 
            # all the job's tasks' subtasks
            for subtask_busy_ratio in subtasks_busy_ratios:
                if subtask_busy_ratio > max_subtask_busy_ratio:
                    max_subtask_busy_ratio = subtask_busy_ratio
        
        return max_subtask_busy_ratio
    
def get_running_job_names():
    """
    Returns a list with the names of the running pyflink jobs
    If no pyflink job is running, returns an empty list ([])
    """
    jobs_endpoint = "http://jobmanager:8081/jobs"
    try:
        jobs_res = requests.get(jobs_endpoint, timeout=5)
    except Exception as e:
        # # Original exception
        # print("Original exception: ", e)
        # print()
        raise Exception("Tried accessing the flink jobs deployed in the cluster. The Flink cluster is not running. Deploy the Flink cluster and rerun the script.")
    
    job_objects = jobs_res.json()["jobs"]
    running_jobs_ids = [job_object["id"] for job_object in job_objects if job_object["status"] == "RUNNING"]
    
    running_job_names = []
    for running_job_id in running_jobs_ids:
        job_info_endpoint = jobs_endpoint + f"/{running_job_id}"
        job_name_res = requests.get(job_info_endpoint)
        job_name = job_name_res.json()["name"]
        running_job_names.append(job_name)
    
    return running_job_names
     
# endregion

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


def produce_from_line_we_left_off(all_events_topic=str, push_events_topic = str, pull_request_events_topic = str, issue_events_topic = str, filepath=str, \
    parsed_files_filepath=str, config=dict):
    '''
    Produces messages from a .json file into a topic
    
    :param topic: the name of the topic to produce messages into
    :param filepath: the path to the *.json.gz file to decompress and read from
    :param parsed_files_dict: dictionary with the files and the line we left off 
    last time we read it
    
    :returns the_whole_file_was_read:
    True: if the whole file was read, False: otherwise (either because of KeyboardInterrupt or because of error)
    :returns the_whole_file_was_read_beforehand:
    True: if the whole file was read, False: otherwise (either because of KeyboardInterrupt or because of error)
    
    '''
    
    filename = os.path.basename(filepath)
    decompressed_file_path = extract_compressed_file_from_path(\
        filepath)
    with open(decompressed_file_path, 'r') as file_object:
        lines_in_file = len(file_object.readlines())   
    parsed_files_dict = restore_parsed_files(parsed_files_filepath)
    # Get the lines where we left off for the file and continue from there
    if filename not in parsed_files_dict.keys():
        parsed_files_dict[filename] = 0    
    line_we_left_off = parsed_files_dict[filename]
    lines_produced = 0
    the_whole_file_was_read = False
    the_whole_file_was_read_beforehand = False
    number_of_lines_produced_per_print = 100000
    time_between_produced_messages = pow(10, -6)
    i = 0 # Line tracker
    
    
    # Read lines of file to be produced in kafka topic until keyboard interrupt
    # or EOF
    # Case 1: The file has not been read yet 
    # or has been read up to a line before the last one
    if line_we_left_off < lines_in_file:
        try:
            
            all_events_producer = Producer(config)
            push_events_producer = Producer(config)
            pull_request_events_producer = Producer(config)
            issue_events_producer = Producer(config)
            
            with open(decompressed_file_path, 'r') as file_object:
                
                lines = file_object.readlines()
                
                # All events in file are of the same day
                event_dict = json.loads(lines[0])
                first_event_dict = event_dict["created_at"]
                event_full_datetime = datetime.strptime(first_event_dict, "%Y-%m-%dT%H:%M:%SZ") 
                event_day = datetime.strftime(event_full_datetime, "%Y-%m-%d")


                print(f"Reading lines of {filename} until EOF or keyboard interrupt...")
                print(f'Producing events from line No.{line_we_left_off+1} of {filename}')
                
                for i in range(line_we_left_off, lines_in_file):    
                    event_dict = json.loads(lines[i])
                    
                    event_str = str({"day": event_day, 
                            "repo": event_dict["repo"]["full_name"],
                            "username": event_dict["actor"],
                            "type": event_dict["type"]})
                    all_events_producer.produce(all_events_topic, value=event_str, callback=delivery_callback)
                    # producer.produce(topic, value=lines[i], callback=delivery_callback)
                    # Poll to cleanup the producer queue after every message production
                    all_events_producer.poll(0)
                    
                        
                    if event_dict["type"] == "PushEvent":
                        event_str = str({"day": event_day,
                                        "repo": event_dict["repo"]["full_name"],
                                        "username": event_dict["actor"], 
                                        "number_of_contributions": event_dict["payload"]["distinct_size"],
                                        "size": event_dict["payload"]["size"],
                                        "distinct_size": event_dict["payload"]["distinct_size"]})
                        push_events_producer.produce(push_events_topic, value= event_str, callback=delivery_callback)
                        push_events_producer.poll(0)
                
                    elif event_dict["type"] == "PullRequestEvent":
                        event_str = str({"day": event_day, 
                                        "repo": event_dict["repo"]["full_name"], 
                                        "username": event_dict["payload"]["pull_request"]["user"],
                                        "number_of_contributions": event_dict["payload"]["pull_request"]["commits"],
                                        "pull_request_number": event_dict["payload"]["pull_request"]["number"],
                                        "opening_time": event_dict["payload"]["pull_request"]["created_at"], 
                                        "closing_time": event_dict["payload"]["pull_request"]["closed_at"], 
                                        "action": event_dict["payload"]["action"],
                                        "merged_at": event_dict["payload"]["pull_request"]["merged_at"]})
                        pull_request_events_producer.produce(pull_request_events_topic, value=event_str, callback=delivery_callback)
                        pull_request_events_producer.poll(0)
                    
                    elif event_dict["type"] == "IssuesEvent":
                        event_str = str({"repo": event_dict["repo"]["full_name"], 
                                    "issue_number": event_dict["payload"]["issue"]["number"],
                                    "opening_time": event_dict["payload"]["issue"]["created_at"],
                                    "closing_time": event_dict["payload"]["issue"]["closed_at"],
                                    "action": event_dict["payload"]["action"],
                                    "labels": event_dict["payload"]["issue"]["labels"]})
                        issue_events_producer.produce(issue_events_topic, value=event_str, callback=delivery_callback)
                        issue_events_producer.poll(0)
            
                    lines_produced = i+1
                        
                    
                    
                    
                    if lines_produced % number_of_lines_produced_per_print == 0:
                        sys.stdout.write("\rJSON objects produced: {0}/{1}".format(lines_produced,lines_in_file))
                        sys.stdout.flush()
                    
                    # # Break production if only 10000 events have been sent
                    # if i >= line_we_left_off + 20000:
                    #     break
                    
                    # Short time to capture output
                    time.sleep(time_between_produced_messages)
                
            # Once the total number of lines were produced, print it
            sys.stdout.write(f"\rJSON objects produced: {i+1}/{lines_in_file}")
            sys.stdout.flush()
                                    
        
        # Case 1.1: Keyboard interrupt while reading the file
        except KeyboardInterrupt:
            print('\nKeyboard interrupt\n')
            the_whole_file_was_read = False
        finally:
            os.remove(decompressed_file_path)
            parsed_files_dict[filename] = lines_produced      
            save_files_parsed(parsed_files_dict, parsed_files_filepath)
            
            # Wait for message delivery
            # all_events_producer.poll(0)
            all_events_producer.flush()
            # push_events_producer.poll(0)
            push_events_producer.flush()
            # pull_request_events_producer.poll(0)
            pull_request_events_producer.flush()
            # issue_events_producer.poll(0)
            issue_events_producer.flush()
            
            
            print('Producers closed properly')
            
        # Case 1.2: EOF 
        # In this case, the file was read up to the last line and
        # no keyboard interrupt occured
        if lines_produced == lines_in_file:
            print('\nEOF\n')
            # Keyboard interrupt did not occur, the whole file was read 
            the_whole_file_was_read = True
            if os.path.exists(decompressed_file_path):
                # Remove the decompressed file
                os.remove(decompressed_file_path)
                        
            
    # Case 2: If the whole file was read before the execution of the script
    # (line_we_left_off = flines_in_file)
    elif line_we_left_off == lines_in_file:
        print('EOF: '
            f'All lines of file {filename} have already been read '
            f'and produced into topics')
        the_whole_file_was_read = True   
        the_whole_file_was_read_beforehand = True
        if os.path.exists(decompressed_file_path):
            # Once done reading the decompressed file, delete it to save space 
            os.remove(decompressed_file_path)
            print()
    # Case 3: An error occurred: (lines_we_left_off > lines_in_file)
    else:
        raise ValueError(f'Lines we read: ({lines_produced}) should be more than the ones '
                        f'already existing in file {decompressed_file_path} '
                        f'({lines_in_file})')
    return the_whole_file_was_read, the_whole_file_was_read_beforehand


        
if __name__ == '__main__':
    
    
    # Performance of pipeline sections and flink jobs
    sections_performance = {"1. Download gharchive file": 0,
                            "2. Thin file": 0,
                            "3. Produce thinned events": 0,
                            "4. Wait for flink jobs to finish": 0,
                            "5. Delete and recreate topic": 0,
                            "Total time elapsed": 0}
    running_job_names_in_cluster = get_running_job_names()
    running_job_names_in_cluster = sorted(running_job_names_in_cluster)
    jobs_completion_times = {job_name: {"starting_time": None, "stopping_time" : None, "time_elapsed" : 0 } for job_name in running_job_names_in_cluster}
    total_dur = 0

    starting_date_formatted =  '2024-12-03-6'
    ending_date_formatted =  '2024-12-03-9' 
    current_date_formatted = starting_date_formatted
    starting_date = datetime.strptime(starting_date_formatted, '%Y-%m-%d-%H')
    ending_date = datetime.strptime(ending_date_formatted, '%Y-%m-%d-%H')
    current_date = starting_date
    

    # Pipeline from starting date to the ending date
    # region
    if starting_date > ending_date:
        raise ValueError(f"Starting date '{starting_date}' should be earlier than the ending date '{ending_date}'")
    
    # Pipeline (download, thin, produce, transform) for all dates from start to end
    while current_date <= ending_date:
        
        # 1. Download gharchive file
        # region
        print(f"\nGharchive file: {current_date_formatted}\n"\
            "1. Download gharchive file:")
        
        st = time.time()
        
        gharchive_file_URL = 'https://data.gharchive.org/' + current_date_formatted + '.json.gz'
        folderpath_to_download_into = '/github_data'
        # Raw events file
        filename = current_date_formatted + '.json.gz'
        filepath_to_download_into = os.path.join(folderpath_to_download_into, filename)
        filepath_of_file_to_thin = filepath_to_download_into
        # Thinned events file
        folderpath_of_thinned_file = folderpath_to_download_into
        thinned_filename = current_date_formatted + '-thinned.json.gz'
        filepath_of_thinned_file = os.path.join(folderpath_to_download_into, thinned_filename)
        # If neither the original or the thinned file exist, download the original to produce it
        if not os.path.exists(filepath_of_file_to_thin) and not os.path.exists(filepath_of_thinned_file):
            download_compressed_GHA_file(gharchive_file_URL, folderpath_to_download_into)
        elif os.path.exists(filepath_of_thinned_file):
            print(f"Thinned file {thinned_filename} already exists. Skipping download")


        et = time.time()
        dur = et - st
        total_dur += dur
        sections_performance["1. Download gharchive file"] += dur        
        # endregion

        # 2. Thin file
        # region
        print("2. Thin file:")

        st = time.time()

        heavy_thin_data_of_file(filepath_of_file_to_thin, filepath_of_thinned_file, delete_original_file=True)
        # thin_data_of_file(filepath_of_file_to_thin, filepath_of_thinned_file)
        
        et = time.time()
        dur = et - st
        total_dur += dur
        sections_performance["2. Thin file"] += dur
        # endregion

        # 3. Produce thinned events
        # region
        print("\n3. Produce thinned events:")
        # Store the files produced and up to which point
        parsed_files_filepath = "/github_data/files_parsed.json"
        the_whole_file_was_read_beforehand = None      
        
        # Get kafka_host:kafka_port
        parser = ArgumentParser()
        parser.add_argument('config_file', type=FileType('r'))
        args = parser.parse_args()
        config_parser = ConfigParser()
        config_parser.read_file(args.config_file)
        config = dict(config_parser['default_producer'])
        config_port = str(config['bootstrap.servers'])
        
        all_events_topic = "all_events"
        push_events_topic = "push_events"
        pull_request_events_topic = "pull_request_events"
        issue_events_topic = "issue_events"
        create_topic_if_not_exists(pull_request_events_topic, config_port)
        create_topic_if_not_exists(issue_events_topic, config_port)
        create_topic_if_not_exists(all_events_topic, config_port)
        create_topic_if_not_exists(push_events_topic, config_port)
        
        # Produce from file into topic
        the_whole_file_was_read, the_whole_file_was_read_beforehand = produce_from_line_we_left_off(all_events_topic, push_events_topic, pull_request_events_topic, issue_events_topic, filepath_of_thinned_file, parsed_files_filepath, config)

        et = time.time()
        dur = et - st
        total_dur += dur
        sections_performance["3. Produce thinned events"] += dur
        # endregion


        # 4. Wait for flink jobs to finish
        # region

        # Set True or False to skip region 
        skip_transformation_region = False
        st = time.time()

        # If the file's events have already been produced in a previous loop iteration, do not wait for jobs to start 
        if skip_transformation_region == False and the_whole_file_was_read_beforehand == False:
            print("\n4. Wait for flink jobs to finish")
            
            # Raise Error is there are no running jobs
            running_job_names_in_cluster = get_running_job_names()
            running_job_names_in_cluster = sorted(running_job_names_in_cluster)
            if running_job_names_in_cluster == []:
                raise Exception("No jobs are running on the Flink cluster. \
                    Execute a job and rerun the producer")
            
            
            # Check if jobs are running
            
            hostname = 'jobmanager'
            
            def wait_for_jobs_to_start(running_job_names_in_cluster, hostname):
                
                jobs_started_time = None
                jobs_are_under_low_load = False
                print(f"Pyflink jobs have not started working.\n"\
                        "Waiting for the jobs to start")            
                is_a_job_running = False # Supposing no job is running initially
                times_waited_before_start = 0
                while(is_a_job_running == False):
                    for single_job_name in running_job_names_in_cluster:
                        is_a_job_running = is_a_job_running or \
                            check_if_job_is_busy(single_job_name, hostname)
                        job_busy_ratio = get_job_busy_ratio(single_job_name,  hostname)
                        sys.stdout.write(f"\rJob: '{single_job_name}', busy ratio: {round(job_busy_ratio*100, 1)}%\n")
                    sys.stdout.flush()
                    if is_a_job_running == True:        
                            print(f"Pyflink jobs have started working")
                            jobs_started_time = time.time()       
                            break
                    time.sleep(5)
                    sys.stdout.write("\033[F" * len(running_job_names_in_cluster))         
                    # If we wait for long (jobs not starting being busy) continue with the next file
                    times_waited_before_start += 1
                    if times_waited_before_start == 3:
                        jobs_are_under_low_load = True
                        break
                
                return jobs_started_time, jobs_are_under_low_load


            jobs_started_time, jobs_are_under_low_load = wait_for_jobs_to_start(
                    running_job_names_in_cluster, hostname)
            
            if jobs_are_under_low_load == True:
                current_date = current_date + timedelta(hours=1)
                current_date_formatted = datetime.strftime(current_date, '%Y-%m-%d-%-H')
                continue
            
            
            def set_job_starting_time(job_name, starting_time, jobs_completion_times):
                '''
                Sets the job's starting time as in jobs_completion_time[job_name]["starting_time"]
                '''
                if jobs_completion_times[job_name]["starting_time"] == None:
                    jobs_completion_times[job_name]["starting_time"] = starting_time
                return jobs_completion_times
            
            # Starting time for jobs 
            for single_job_name in running_job_names_in_cluster:   
                jobs_completion_times = set_job_starting_time(single_job_name, jobs_started_time, jobs_completion_times)
                
            
            def wait_for_all_jobs_to_start_running(running_job_names_in_cluster, hostname):
                are_all_jobs_running = False
                print("Waiting for all jobs to start running")
                while(are_all_jobs_running == False):
                    are_all_jobs_running = True
                    # If one job is not running, are_all_jobs_running will become false
                    for single_job_name in running_job_names_in_cluster:
                        are_all_jobs_running = are_all_jobs_running and check_if_job_is_busy(single_job_name, hostname)
                    time.sleep(3)
                
                        
            # Uncomment to wait for all jobs to start running
            # This is used to measure the jobs' performances
            wait_for_all_jobs_to_start_running(running_job_names_in_cluster, hostname)
            
            all_kafka_topics = [all_events_topic, push_events_topic, \
                pull_request_events_topic, issue_events_topic]
            
            def wait_for_jobs_to_stop(running_job_names_in_cluster, hostname, jobs_completion_times, all_kafka_topics):
                #  While there is at least one working job, wait for it to finish
                wait_for_busy_jobs = False      
                set_explicit_wait_for_busy_jobs = False # Set true to wait for jobs to complete
                bootstrap_servers = get_kafka_broker_config(all_events_topic)        
                max_number_of_messages = 10000000
                number_of_messages = 0
                max_job_busy_ratio_threshold = 0.7
                
                for topic in all_kafka_topics:
                    number_of_messages = max(number_of_messages, \
                        get_topic_number_of_messages(topic, bootstrap_servers))        
                # Conditions to wait for jobs to complete
                if set_explicit_wait_for_busy_jobs == True:
                    wait_for_busy_jobs = True      
                    print("Explicit set to wait for all jobs to finish.\n"\
                        "Waiting for pyflink jobs to stop completely")      
                elif number_of_messages > max_number_of_messages:
                    wait_for_busy_jobs = True
                    print("Max number of messages of topic reached. "\
                        "Topic messages must be deleted.\n"\
                        "Waiting for pyflink jobs to stop completely")
                elif current_date == ending_date:
                    wait_for_busy_jobs = True
                    print("Producing messages from the last file in the time range \n"\
                        "Waiting for pyflink jobs to stop completely")
                if wait_for_busy_jobs == False:
                    print(f"Waiting for busiest pyflink job's busy ratio "\
                        f"to drop from {max_job_busy_ratio_threshold*100}%% "\
                        "before producing new messages") 
                
                jobs_busy_ratios = {}
                is_a_job_running = True
                while(is_a_job_running == True):
                    is_a_job_running = False # Reset the job status.
                    for single_job_name in running_job_names_in_cluster:        
                        is_a_job_running = is_a_job_running or \
                            check_if_job_is_busy(single_job_name, hostname)
                        job_busy_ratio = get_job_busy_ratio(single_job_name, hostname)
                        
                        
                        def set_job_stopping_time(job_name, jobs_completion_times):
                            jobs_completion_times[job_name]["stopping_time"] = time.time()
                            jobs_completion_times[job_name]["time_elapsed"] += \
                                jobs_completion_times[job_name]["stopping_time"] - \
                                    jobs_completion_times[job_name]["starting_time"]
                            return jobs_completion_times
                            
                            
                        # Stop the timer (stopping_time and time_elapsed) of stopped jobs
                        # After that, ending time cannot be updated again until the next time all jobs start again
                        if job_busy_ratio == 0 and jobs_completion_times[single_job_name]["stopping_time"] == None:
                            # jobs_completion_times[single_job_name]["stopping_time"] = time.time()
                            # jobs_completion_times[single_job_name]["time_elapsed"] += \
                            #     jobs_completion_times[single_job_name]["stopping_time"] - \
                            #         jobs_completion_times[single_job_name]["starting_time"]
                            jobs_completion_times = set_job_stopping_time(single_job_name, jobs_completion_times)
                            
                        jobs_busy_ratios[single_job_name] = job_busy_ratio
                        sys.stdout.write(f"\rJob: '{single_job_name}', busy ratio {round(job_busy_ratio*100, 1)}%\n")
                    sys.stdout.flush()
                    max_job_busy_ratio = max(jobs_busy_ratios.values())
                    
                    # If the jobs stopped (busy ratio 0%) or are not at busy ratio max_job_busy_ratio_threshold*100%, continue producing messages
                    if (is_a_job_running == False or \
                        max_job_busy_ratio < max_job_busy_ratio_threshold) \
                        and wait_for_busy_jobs == False:
                        print(f"\nPyflink jobs stopped or are not "\
                            f"{round(max_job_busy_ratio_threshold*100)}% busy. "\
                            "Can continue producing messages")
                        break
                    # Only if the jobs stopped (busy ratio 0%), continue producing messages
                    elif is_a_job_running == False and wait_for_busy_jobs == True:
                        print("Pyflink jobs stopped.")
                        break
                    else: 
                        # If no job stopped or busy ratio dropped, keep waiting
                        pass
                    time.sleep(5)
                    
                    # Get the new running jobs in case one was added to the cluster or cancelled
                    running_job_names_in_cluster = get_running_job_names()
                    running_job_names_in_cluster = sorted(running_job_names_in_cluster)
                    
                    sys.stdout.write("\033[F" * len(running_job_names_in_cluster))  
                return jobs_completion_times
            
                
            jobs_completion_times = wait_for_jobs_to_stop(running_job_names_in_cluster, hostname, \
                jobs_completion_times, all_kafka_topics)
            
            
            def reset_starting_and_stopping_times_of_stopped_job(job_name, jobs_completion_times):
                '''
                Reset the starting and stopping time of jobs if stopped
                '''
                if jobs_completion_times[job_name]["stopping_time"] != None: # Stopped job
                    jobs_completion_times[job_name]["starting_time"] = None
                    jobs_completion_times[job_name]["stopping_time"] = None
                return jobs_completion_times
                
            # Reset the timer for stopped jobs
            for single_job_name in running_job_names_in_cluster:        
                jobs_completion_times = reset_starting_and_stopping_times_of_stopped_job(single_job_name, \
                    jobs_completion_times)
        
        et = time.time()
        dur = et - st
        total_dur += dur
        sections_performance["4. Wait for flink jobs to finish"] += dur
        # endregion
        
        
        # 5. Delete and recreate the topic if too large
        # region
        # Set True or False to skip region
        skip_delete_topic = True
        st = time.time()
        
        if skip_delete_topic == False:
            # Delete and recreate topics if too large
            bootstrap_servers = get_kafka_broker_config(topic)
            for topic in [all_events_topic, push_events_topic, pull_request_events_topic, issue_events_topic]:
                number_of_messages = get_topic_number_of_messages(topic, bootstrap_servers)
                max_number_of_messages = 2000000    
                delete_topic_if_full(topic, max_number_of_messages, bootstrap_servers)
                # Short delay to update kafka cluster metadata before recreating the topic
                time.sleep(5)
                create_topic_if_not_exists(topic, bootstrap_servers)
    
        et = time.time()
        dur = et - st
        total_dur += dur
        sections_performance["5. Delete and recreate topic"] += dur
        # endregion
        
        current_date = current_date + timedelta(hours=1)
        current_date_formatted = datetime.strftime(current_date, '%Y-%m-%d-%-H')
    
        
    sections_performance["Total time elapsed"] = total_dur
    print("Execution times of pipeline parts in seconds:")
    for k, v in sections_performance.items():
        print(f"{k}: {round(v, 1)}")

    print("\nTotal busy times of jobs:")
    for k, v in jobs_completion_times.items():
        temp_time_to_complete = round(v["time_elapsed"], 1)
        print(f"{k}: {temp_time_to_complete}")









# Test to increase the topic partitions
    # topic = 'historical-raw-events'
    # bootstrap_servers = get_kafka_broker_config(topic)
    # client = admin.AdminClient({"bootstrap.servers": bootstrap_servers})

    # # # Create topic
    # # create_topic_future = client.create_topics([topic])
    # # print(create_topic_future)
    # # print(create_topic_future[topic])
    # # print(create_topic_future[topic].result())
    
    # # # Delete topic
    # # delete_topic_future = client.delete_topics([topic])
    # # print(delete_topic_future[topic].result())
    
    
    # # Increase topic partition
    # number_of_partitions = 4
    # new_partitions = admin.NewPartitions(topic, new_total_count=number_of_partitions)
    # create_topic_partitions_future = client.create_partitions([new_partitions])
    
    # print(create_topic_partitions_future[topic].result())