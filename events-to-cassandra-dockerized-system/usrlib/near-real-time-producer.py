from produce_from_last_line_of_file import produce_from_last_line_of_file
from datetime import datetime, timedelta
import requests
import os
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from create_topic_from_inside_the_container import create_topic_from_within_container

from thin_data import thin_data_of_file

from check_job_status_multiple_jobs import check_if_job_is_busy

import time

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
        r = requests.get(gha_file_url, stream=True)
        print('Starting download of GHArchive file from URL: {}'.format(gha_file_url))
        with open(filepath, 'wb') as f:
            for chunk in r.raw.stream(1024, decode_content=False):
                if chunk:
                    f.write(chunk)
        print("GHArchive file from URL {} finished downloading".format(gha_file_url))
    else:
        print(f"File {filename} already exists in folder {folderpath}.")
    
    
if __name__=='__main__':
    # Get the URL of the latest gharchive available 
    newest_URL, newest_gharchive_file_date = get_newest_GHArchive_file_URL() 

    # In docker 
    folderpath_to_download_into = '/github_data_near_real_time'
    

    # Download latest gharchive file
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
    parsed_files_filepath = "/github_data_near_real_time/files_parsed.json"

    topic_to_produce_into = 'near-real-time-raw-events'
    # # Path with custom events
    # filepath = '/home/xeon/Thesis/local-kafka-flink-cassandra-integration/presentation-10-demo/task-1-preprocess-download-and-thin-data/og_vs_thinned_events/2024-01-01-0-thinned-oneliner-custom-events.json.gz'

    the_whole_file_was_produced_already = produce_from_last_line_of_file(topic_to_produce_into, filepath_of_thinned_file, parsed_files_filepath)




    # # Wait for the pyflink jobs to finish executing
    # # The pyflink jobs derive stars, forks, daily stats and popularity insights
    # stars_and_forks_busy_status = check_if_job_is_busy('Get stars and forks')
    # stats_and_popularity_insights_busy_status = check_if_job_is_busy(\
    #                         "Get real time stats and popularity insights")


    # # If the whole latest gharchive file has been produced (or you can wait until transformation stops)
    # if (the_whole_file_was_produced_already == True):
    #     print("The whole latest gharchive file has been read.\n"
    #           "Waiting for pyflink jobs to complete")
    #     quit()

    # # If the whole latest gharchive file has just been produced
    # # Check if the jobs on the near-real-time-raw-events have terminated
    # else:
    #     if (stars_and_forks_busy_status == False and \
    #         stats_and_popularity_insights_busy_status == False):
            
    #         # Wait until datastream transformation operators start working
    #         while(stars_and_forks_busy_status == False or \
    #             stats_and_popularity_insights_busy_status == False):
                
    #             print("Waiting for pyflink jobs to complete datastream transformation")
    #             time.sleep(5)
    #             stars_and_forks_busy_status = check_if_job_is_busy('Get stars and forks')
    #             stats_and_popularity_insights_busy_status = check_if_job_is_busy(\
    #                                 "Get real time stats and popularity insights")
    #     else:
    #         # Wait until transform stops
    #         while(stars_and_forks_busy_status == True or \
    #             stats_and_popularity_insights_busy_status == True):
                
    #             print("Waiting for transformation of data to stop")
    #             time.sleep(5)
    #             stars_and_forks_busy_status = check_if_job_is_busy('Get stars and forks')
    #             stats_and_popularity_insights_busy_status = check_if_job_is_busy(\
    #                                 "Get real time stats and popularity insights")


    # # By this point, the execution must have stopped and 
    # # data should now be able to be added into Cassandra through a consumer of the datastreams.