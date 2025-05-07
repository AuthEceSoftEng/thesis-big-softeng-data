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

from requests_sse import EventSource
import sys
    
if __name__=='__main__':
    
    # Pipeline steps
    # Get the data 
    # Thin incoming data 
    # - keep only fields you need (see the jobs creating the extra topics)
    # Produce data into topic
    # (Optional) Monitor the jobs busy ratios to keep track of performance 


    # topic_to_produce_into = 'near-real-time-raw-events'
    # the_whole_file_was_produced_already = produce_from_last_line_of_file(topic_to_produce_into, filepath_of_thinned_file, parsed_files_filepath)

    event_counter = 0
    max_num_of_events_to_print = 1
    with EventSource('http://github-firehose.libraries.io/events', timeout=30) as event_source:
        for event in event_source:
            try:
                event_counter += 1
                print(f"Got event No{event_counter}: {event}")
                print(f"Type of event: {type(event)}")
                
                if event_counter >= max_num_of_events_to_print:
                    sys.exit(f"Reached max number of events to print: "\
                        f"{max_num_of_events_to_print}")
            except KeyboardInterrupt:
                sys.exit("Keyboard interrupt")
            