import requests

def get_running_job_ids(flink_local_port):
    """
    Returns a list with the names of the running pyflink jobs
    If no pyflink job is running, returns an empty list ([])
    """
    jobmanager_name = 'jobmanager'
    jobs_endpoint = f"http://{jobmanager_name}:{flink_local_port}/jobs"
    try:
        jobs_res = requests.get(jobs_endpoint, timeout=5)
    except Exception as e:
        # # Original exception
        # print("Original exception: ", e)
        # print()
        raise Exception("Tried accessing the flink jobs deployed in the cluster. The Flink cluster is not running. Deploy the Flink cluster and rerun the script.")
    
    job_objects = jobs_res.json()["jobs"]
    running_jobs_ids = [job_object["id"] for job_object in job_objects if job_object["status"] == "RUNNING"]
    if running_jobs_ids == []:
        print("There are no RUNNING jobs to cancel")
    
    job_id_to_name_dict = {}
    
    running_job_names = []
    for running_job_id in running_jobs_ids:
        job_info_endpoint = jobs_endpoint + f"/{running_job_id}"
        job_name_res = requests.get(job_info_endpoint)
        job_name = job_name_res.json()["name"]
        job_id_to_name_dict[running_job_id] = job_name
        # running_job_names.append(job_name)
        
    return job_id_to_name_dict


# Get all flink job ids
flink_local_port = 8081
job_id_to_name_dict = get_running_job_ids(flink_local_port)

# Command to cancel all running jobs in the cluster
jobmanager_name = "jobmanager"

for job_id in job_id_to_name_dict.keys():
    cancel_job_endpoint = f"http://{jobmanager_name}:{flink_local_port}/jobs/{job_id}"
    print(f"Canceling job '{job_id_to_name_dict[job_id]}' with id {job_id}...")
    patch_res = requests.patch(cancel_job_endpoint)
    print("Done")
