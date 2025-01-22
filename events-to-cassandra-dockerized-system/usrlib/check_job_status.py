

# Extract Job ID

import requests


def get_subtasks_endpoints_of_job_in_flink():
    """
    Returns a list with the URL endpoints of the subtasks of a
    flink job's tasks.
    
    Assumptions: 
    - Port of localhost where the Flink web UI is deployed is locahost:8081
    - Only a single job is running at a time in the Flink cluster
    - Responses of endpoints are according to th Flink REST API:
    https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/rest_api/#api
    
    Example: returns [[0, 0, 0, 1], [0.3, 0.3, 0.1, 0.9], [0.5, 0.4, 0.2, 0.8]]
    which means that the pyflink job contains 3 tasks with 4 subtasks each
    with each one having the aforementioned busyRatios
    """
    # For secure connection:
    # Setup SSL for the Flink REST API: 
    # see: https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/security/security-ssl/#configuring-ssl
    # For for local use and for simplicity reasons no ssl is used.

    all_jobs_ids_url = "http://localhost:8081/jobs/"
    res = requests.get(all_jobs_ids_url)
    if res.status_code != 200:
        raise ValueError(f"Endpoint {all_jobs_ids_url} could not be accessed\n"
                        f"HTTP status code: {res.status_code}")
    
    
    # The jobs response of the endpoint in JSON format
    jobs_json_dict = res.json()["jobs"]

    # Get the running jobs' IDs
    jobs_id_list = [job_element["id"] for job_element in jobs_json_dict if job_element["status"]=="RUNNING"]

    # # Print JobIDs
    # print(jobs_id_list)

    # Assumption: returned job IDs of the endpoint should be a list of 
    # a single element meaning only one running job.
    if len(jobs_id_list) >=2:
        raise ValueError(f"Endpoint {all_jobs_ids_url} returned more than one job\n"
                        "Cancel one from the web UI and rerun the script")
    elif len(jobs_id_list) == 0:
        raise ValueError(f"No jobs are running on Flink."
                        "Execute a pyflink job and rerun the program")

    job_id = jobs_id_list[0]

    # Extract all vertices meaning all tasks of the single job we found beforehand
    vertices_url = "http://localhost:8081/jobs/" + job_id
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
    Checks if the subtasks retrieved from the subtasks_endpoint are busy
    
    ``tasks_endpoint``: HTTP endpoint to call to retrieve the subtasks 
    of all tasks in the pyflink job
    ``show_endpoint``: prints the endpoint for the task 
    ``verbose``: prints the subtasks busyRatios
    
    returns: True if the task is busy (meaning its subtasks are busy) 
    and
    False if it is not (meaning all its subtasks are idle/not busy)
    '''
    res = requests.get(task_endpoint)
    subtasks_of_task_list = res.json()["subtasks"]
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


def check_if_job_is_busy(show_endpoint=False, verbose=False):
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
    

# # Demo to access the subtasks' busyRatios            
# if check_if_job_is_busy(verbose=False) == True:
#     print("Job is still processing elements")
# else:
#     print("Job is no longer processing elements")





