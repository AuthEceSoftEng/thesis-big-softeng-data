'''
Functions to check whether one or more pyflink jobs are running in the Flink UI
'''

# Extract Job ID

import requests


def get_subtasks_endpoints_of_job_in_flink(job_name):
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
    all_jobs_ids_url = "http://localhost:8081/jobs/"
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
        running_job_id = "http://localhost:8081/jobs/" + job_id
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
    vertices_url = "http://localhost:8081/jobs/" + id_of_the_wanted_job
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


def check_for_busy_jobs(show_endpoint=False, verbose=False):
    '''
    TODO: 
    - get_subtasks_endpoints_of_job_in_flink() - transform for multiple jobs
    - check_if_subtasks_of_tasks_are_busy() - transform for multiple jobs
    '''
    
    
    """
    Checks if the running Flink job is busy. 
    Assumption (see function 'get_subtasks_endpoints_of_job_in_flink'):
    The job is busy if there is at least one subtask of one task of the job that 
    has busyRatio != 0 (is busy) 
    """
    
    # Get the subtasks endpoints for all tasks in the flink job
    subtask_of_task_endpoint_list = get_subtasks_endpoints_of_job_in_flink() 
    
    # If one task of the job is busy return True
    for subtask_of_task_endpoint in subtask_of_task_endpoint_list:
        if check_if_subtasks_of_task_are_busy(subtask_of_task_endpoint,\
            show_endpoint, verbose) == True:
            return True
    # If no task is busy return False
    return False
    

def check_if_job_is_busy(job_name, show_endpoint=False, verbose=False):
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
    subtask_of_task_endpoint_list = get_subtasks_endpoints_of_job_in_flink(job_name) 
    
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


# # Demo to access the subtasks' busyRatios            
# if check_if_job_is_busy(verbose=False) == True:
#     print("Job is still processing elements")
# else:
#     print("Job is no longer processing elements")





