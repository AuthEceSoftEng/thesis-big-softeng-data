# from produce_from_last_line_of_file import produce_from_last_line_of_file
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, admin


# from create_topic_from_inside_the_container import create_topic_from_within_container

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

# from delete_and_recreate_topic import get_kafka_broker_config, get_topic_number_of_messages, create_topic_if_not_exists, delete_topic_if_full


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

        
if __name__ == '__main__':
    
    # Get the URL of the gharchive available you want to 
    starting_date_formatted =  '2024-12-04-10'
    ending_date_formatted =  '2024-12-04-14' 

    current_date_formatted = starting_date_formatted
    starting_date = datetime.strptime(starting_date_formatted, '%Y-%m-%d-%H')
    ending_date = datetime.strptime(ending_date_formatted, '%Y-%m-%d-%H')
    current_date = starting_date
    
    # Performance of pipeline sections: Download, Thin events, produce and transform data
    sections_performance = {"1. Download gharchive file": 0,
                            "2. Thin file": 0,
                            "Total time elapsed": 0}
    
    
    total_dur = 0

    # Pipeline from starting date to the ending date
    # region
    # Check dates validity
    if starting_date > ending_date:
        raise ValueError(f"Starting date '{starting_date}' should be earlier than the ending date '{ending_date}'")
    
    # Pipeline (download, thin, produce, transform) for all dates from start to end
    while current_date <= ending_date:
        
        # 1. Download gharchive file
        # region
        print(f"\nGharchive file: {current_date_formatted}")
        print("1. Download gharchive file")
        
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

        current_date = current_date + timedelta(hours=1)
        current_date_formatted = datetime.strftime(current_date, '%Y-%m-%d-%-H')
            
    sections_performance["Total time elapsed"] = total_dur
    print("Execution times of pipeline parts in seconds:")
    for k, v in sections_performance.items():
        print(f"{k}: {round(v, 1)}")
