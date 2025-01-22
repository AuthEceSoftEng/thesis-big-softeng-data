from produce_from_last_line_of_file import produce_from_last_line_of_file, create_topic
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, admin


from create_topic_from_inside_the_container import create_topic_from_within_container

from thin_data import thin_data_of_file
import time, sys
# from check_job_status_multiple_jobs import check_if_job_is_busy 

# from check_job_status_multiple_jobs  import get_subtasks_endpoints_of_job_in_flink

import json
import subprocess
from datetime import datetime, timedelta
import requests
import os



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
    res = requests.get(task_endpoint)
    try:
        subtasks_of_task_list = res.json()["subtasks"]
    except KeyError as e:
        print(f"Task endpoint: {task_endpoint}")
        print(f"Res.json(): {res.json()}")
        print(e)
        raise KeyError
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
    
# Delete messages from a topic
# region


def create_config_file_to_delete_messages_of_topic(topic_name, folderpath):
    """
    Creates a .json configuration file to delete the messages of a topic
    
    ``topic_name``: The topic name
    ``folderpath``: The folder to store the configuration .json file 
    to delete the topic's messages
    
    returns: The filepath of the created configuration file 
    """
    
    # Create filename
    filename = f"delete_messages_of_topic_{topic_name}.json"
    
    # Create filepath
    delete_messages_configs_filepath = os.path.join(folderpath, filename)
    
    
    with open(delete_messages_configs_filepath, 'w') as config_file:
        configs_to_delete_messages_of_topic_dict = \
            {"partitions":[{"topic": f"{topic_name}","partition": 0,"offset": -1}],\
                "version": 1}
        json.dump(configs_to_delete_messages_of_topic_dict, config_file)
        
    return delete_messages_configs_filepath


def purge_topic_via_command(topic=str, config_server_and_port=str, \
    topic_config_in_kafka_container=str, kafka_container_name=str):
    """
    Purges a topic using the command kafka-delete-records.sh
    The command takes a json as argument containing the topic name
    
    ``topic``: Topic whose messages are to be deleted
    ``config_server_and_port``: Host and port (e.g. localhost:34760) of the kafka 
    broker hosting the topic
    ``out_of_docker_config_filepath``: Folderpath to the configuration file that deletes the 
    messages of the topic
    
    returns: nothing
    """

    # Create the configuration file to delete its 
    # messages (the config file contains the topic's name) 
    config_filepath = create_config_file_to_delete_messages_of_topic(topic, \
        topic_config_in_kafka_container)
    
    
    # Command to mount the 'delete messages configuration file' into the docker container
    config_filename = os.path.basename(config_filepath)
    copy_delete_messages_config_command = \
        f"docker cp {config_filepath} \
        {kafka_container_name}:/home/{config_filename}"

  
    # Execute the command
    subprocess.run(copy_delete_messages_config_command, shell=True)
    
    # Move to the directory containing /bin with the kafka commands
    cd_command = "cd ../.."

    # Purge messages command
    purge_topic_command = f"kafka-delete-records --bootstrap-server\
        {config_server_and_port} --offset-json-file \
            /home/{config_filename}"
    
    
    # Serialize the commands to execute consecutively
    docker_commands_to_execute = "; ".join([cd_command, purge_topic_command])
    
    # Docker command to get into the container and execute 
    # the aforementioned commands
    final_docker_command = f"docker exec -i {kafka_container_name} bash -c\
        '{docker_commands_to_execute}'"
  
    # Run commands in terminal
    subprocess.run(final_docker_command, shell=True)
    
    
def get_size_of_kafka_topic_in_docker_container(topic=str, config_server_and_port=str, kafka_container_name=str):
    """
    Returns the topic size of a topic in a kafka session deployed in a docker container
    """
    docker_commands_to_execute = f"/bin/kafka-log-dirs --bootstrap-server {config_server_and_port} --describe --topic-list {topic} | grep size"
    
    final_docker_command = f"docker exec -i {kafka_container_name} bash -c\
        '{docker_commands_to_execute}'"
  
    # Run commands in terminal
    dict_with_topic_size_stringified = subprocess.run(final_docker_command, shell=True, capture_output=True)
    
    dict_with_topic_size_stringified = dict_with_topic_size_stringified.stdout.decode('utf-8')
    
    dict_with_topic_size = json.loads(dict_with_topic_size_stringified)
    
    topic_size = dict_with_topic_size["brokers"][0]["logDirs"][0]["partitions"][0]["size"]
    return topic_size


def purge_topic_via_configuration(topic=str, config_server_and_port=str, kafka_container_name=str):
    '''
    Configures the topic and the broker
    in order to delete the messages in the topic
    
    ``topic``: the topic whose messages we want to delete
    ``config_server_and_port``: the server and port of the kafka cluster (e.g. localhost:42880)
    '''
    
    # Move to the directory containing /bin with the kafka commands
    cd_command = "cd ../.."
    
    # Configure the topic
    topic_configs_list = ["cleanup.policy=delete", "delete.retention.ms=1", \
        "local.retention.bytes=1", "local.retention.ms=1", "retention.bytes=1", "retention.ms=1", 
        "segment.bytes=1000", "segment.ms=1"]
    
    topic_configs_formatted  = ",".join(topic_configs_list)
    
    topic_configs_command = f"bin/kafka-configs  \
        --bootstrap-server {config_server_and_port} \
        --entity-type topics  --alter --entity-name {topic}  \
        --add-config \
        {topic_configs_formatted}"
        
    
    # Configure the broker
    broker_configs_list = ["log.roll.ms=1000"]
    
    broker_configs_formatted = ",".join(broker_configs_list)
    
    broker_configs_command = f"kafka-configs \
        --bootstrap-server {config_server_and_port} \
        --entity-type brokers \
        --entity-name 1 \
        --alter --add-config \
        {broker_configs_formatted}"
    
    # Serialize the commands to execute consucutively
    docker_commands_to_execute = "; ".join([cd_command, broker_configs_command, \
                                            topic_configs_command])
    
    # Docker command to get into the container and execute 
    # the aforementioned commands
    final_docker_command = f"docker exec -i {kafka_container_name} bash -c \
        '{docker_commands_to_execute}'"
    
    # Run commands in terminal
    subprocess.run(final_docker_command, shell=True)


def restore_default_configs_of_topic(topic=str, config_server_and_port=str, kafka_container_name=str):
    
    # Move to the directory containing /bin with the kafka commands
    cd_command = "cd ../.."
    
    topic_configs_list = ["cleanup.policy", "delete.retention.ms", "local.retention.bytes", \
        "local.retention.ms", "retention.bytes", "retention.ms", "segment.bytes",\
        "segment.ms"]
    
    topic_configs_formatted  = ",".join(topic_configs_list)
    
    # Configure the topic
    topic_configs_command = f"bin/kafka-configs \
        --bootstrap-server {config_server_and_port} \
        --entity-type topics --entity-name {topic} \
        --alter --delete-config \
        {topic_configs_formatted}"
    
    
    # Configure the broker
    broker_configs_list = ["log.roll.ms"]
    
    broker_configs_formatted  = ",".join(broker_configs_list)
    
    broker_configs_command = f"kafka-configs \
        --bootstrap-server {config_server_and_port} \
        --entity-type brokers \
        --entity-name 1 \
        --alter --delete-config \
        {broker_configs_formatted}"
    
    # Serialize the commands to execute consucutively
    docker_commands_to_execute = "; ".join([cd_command, broker_configs_command, \
                                            topic_configs_command])
    
    # Docker command to get into the container and execute 
    # the aforementioned commands
    final_docker_command = f"docker exec -i {kafka_container_name} bash -c\
        '{docker_commands_to_execute}'"
    
    # Run commands in terminal
    subprocess.run(final_docker_command, shell=True)


def free_up_topic_space(topic_config_in_kafka_container, topic, config_server_and_port, kafka_container_name, wait_for_container_size_to_reduce_to_0=False):

    purge_topic_via_command(topic, config_server_and_port, topic_config_in_kafka_container)
    
    
    # Get size of topic before the deletion of messages
    size_of_topic = get_size_of_kafka_topic_in_docker_container(topic, config_server_and_port, kafka_container_name)
    print(f"Size of topic {topic}: {size_of_topic}\n")
    
    # Delete messages through configuration
    print(f"Freeing up space taken by topic...")
    purge_topic_via_configuration(topic, config_server_and_port)
    
    if wait_for_container_size_to_reduce_to_0 == False:
        return
        
    # Wait while size of topic becomes 0
    while size_of_topic != 0:
        sys.stdout.write(f"\r Waiting for the kafka broker to free up the topic's space...")
        sys.stdout.flush()
        time.sleep(5)
        size_of_topic = get_size_of_kafka_topic_in_docker_container(topic, config_server_and_port, kafka_container_name)

        
    restore_default_configs_of_topic(topic, config_server_and_port)
    print("Freed up space in kafka broker")
    purge_topic_via_command(topic, config_server_and_port, topic_config_in_kafka_container)
    
    # Get size of topic before the deletion of messages
    size_of_topic = get_size_of_kafka_topic_in_docker_container(topic, config_server_and_port, kafka_container_name)
    print(f"Size of topic {topic}: {size_of_topic}\n")
    
    # Delete messages through configuration
    print(f"Freeing up space taken by topic...")
    purge_topic_via_configuration(topic, config_server_and_port)
    
    # Wait while size of topic becomes 0
    while size_of_topic != 0:
        sys.stdout.write(f"\rWaiting for the kafka broker to free up the topic's space... Current topic size: {size_of_topic}")
        sys.stdout.flush()
        time.sleep(5)
        size_of_topic = get_size_of_kafka_topic_in_docker_container(topic, config_server_and_port, kafka_container_name)
# end_region    
        
        
if __name__ == '__main__':
    
    
    # # Get the URL of the gharchive available you want to 
    starting_date_formatted =  '2024-11-01-1'
    ending_date_formatted =  '2024-11-01-2'
    
    
    
    starting_date = datetime.strptime(starting_date_formatted, '%Y-%m-%d-%H')
    ending_date = datetime.strptime(ending_date_formatted, '%Y-%m-%d-%H')
    current_date = starting_date
    
    # Produce all gharchive files' events from the starting date up to the ending date
    while current_date <= ending_date:
        
        current_date_formatted = datetime.strftime(current_date, '%Y-%m-%d-%-H')
        gharchive_file_URL = 'https://data.gharchive.org/' + current_date_formatted + '.json.gz'
        

        # time.sleep(100)
        # input("Get into the container and check if the files_parsed.json exists inside userlib/github_data... Press a key to continue") 
        
        # folderpath_to_download_into = '/usrlib/github_data'
        folderpath_to_download_into = '/github_data'

        # Download latest gharchive file
        download_compressed_GHA_file(gharchive_file_URL, folderpath_to_download_into)

        # Thin data
        # Input
        filename = current_date_formatted + '.json.gz'
        filepath_to_download_into = os.path.join(folderpath_to_download_into, filename)
        filepath_of_file_to_thin = filepath_to_download_into

        # Output
        folderpath_of_thinned_file = folderpath_to_download_into
        thinned_filename = current_date_formatted + '-thinned.json.gz'
        filepath_of_thinned_file = os.path.join(folderpath_to_download_into, thinned_filename)

        thin_data_of_file(filepath_of_file_to_thin, filepath_of_thinned_file)


        # Out of docker
        parsed_files_filepath = "/github_data/files_parsed.json"

        topic_to_produce_into = 'historical-raw-events'
        
        
        the_whole_file_was_produced_already = None
        the_whole_file_was_read_beforehand = None
                
        # If the file was read beforehand, then the producer will not produce more events
        the_whole_file_was_produced_already, the_whole_file_was_read_beforehand = produce_from_last_line_of_file(topic_to_produce_into, filepath_of_thinned_file, parsed_files_filepath)

        if the_whole_file_was_read_beforehand:
            current_date = current_date + timedelta(hours=1)
            continue
    
        

        current_date = current_date + timedelta(hours=1)
        
        
        
    # Wait for the pyflink jobs to finish executing
    # region
    running_job_names_in_cluster = get_running_job_names()
        
    if running_job_names_in_cluster == []:
        raise Exception("No jobs are running on the Flink cluster. Execute a job and rerun the producer")

    
    # # Uncomment code below if all the jobs should be deployed
    # # in the cluster
    # names_of_jobs_that_should_be_running = ["screen_2_q6_q8_via_flink_local_run", "screen_3_q9_q10_via_flink_local_run", "screen_4_q11_q15_via_flink_local_run"]
    # for name_of_job_that_should_be_running in names_of_jobs_that_should_be_running:
    #     if name_of_job_that_should_be_running not in running_job_names_in_cluster:
    #         raise Exception("Not all jobs are running in the Flink cluster")
    
    
    hostname = 'jobmanager'
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
    
        
    # endregion





        
