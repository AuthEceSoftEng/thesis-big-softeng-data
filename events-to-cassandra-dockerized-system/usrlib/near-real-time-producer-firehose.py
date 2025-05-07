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
import json


from collections import defaultdict


def heavy_process_json_event(event):
	"""
	Function that receives as input an event and keeps only fields of interest.
	"""
	
	new_event = defaultdict(dict)
	try:
		new_event = {
			"type": event["type"],	
			"repo": {
	   			"full_name": event["repo"]["name"]
		  	},
			"created_at": event["created_at"]
		}
		if new_event["type"] == "PushEvent":
			new_event["payload"] = {
		   		"size": event["payload"]["size"],
				"distinct_size": event["payload"]["distinct_size"],
			}		
			
		elif new_event["type"] == "IssuesEvent":
			new_event["payload"] = {
				"action": event["payload"]["action"]		
			}
		elif new_event["type"] in ["WatchEvent", "ForkEvent"]:
			new_event["username"] = {
				"username": event["actor"]["login"]		
			}
		elif new_event["type"] in ["PullRequestEvent"]:
			new_event["payload"] = {
				"action": event["payload"]["action"],
				"language": event["payload"]["pull_request"]["base"]["repo"]["language"],
				"topics": event["payload"]["pull_request"]["base"]["repo"]["topics"]
			}
		elif new_event["type"] in ["PullRequestReviewEvent", \
	  			"PullRequestReviewCommentEvent"]:
			new_event["payload"] = {
				"language": event["payload"]["pull_request"]["base"]["repo"]["language"],
				"topics": event["payload"]["pull_request"]["base"]["repo"]["topics"]
			}
	
	except KeyError as e:
		raise KeyError(e)
  
	return new_event



if __name__=='__main__':
	
	# Pipeline steps
	# Get the data 
	# Thin incoming data 
	# - keep only fields you need (see the jobs creating the extra topics)
	# Produce data into topic
	# (Optional) Monitor the jobs busy ratios to keep track of performance 


	# topic_to_produce_into = 'near-real-time-raw-events'
	# the_whole_file_was_produced_already = produce_from_last_line_of_file(topic_to_produce_into, filepath_of_thinned_file, parsed_files_filepath)

	try:
		event_counter = 0
		max_num_of_events_to_print = 10
		st = time.time()
		with  EventSource('http://github-firehose.libraries.io/events', timeout=30) as event_source:
			for event in event_source:
				event_counter += 1
				event_data = json.loads(event.data) 
    
				event_data_thinned = heavy_process_json_event(event_data)
				print(f"Got thinned event data No {event_counter}.: {event_data_thinned}")
				print(f"Type of thinned event data: {type(event_data_thinned)}")
				
    
				# event_data_thinned = {k:event_data[k] for k in ['type']}
				# print(f"Got thinned event data No {event_counter}.: {event_data_thinned}")
				# print(f"Type of thinned event data: {type(event_data_thinned)}")
				
				if event_counter >= max_num_of_events_to_print:
					sys.exit(f"Reached max number of events to print: "\
						f"{max_num_of_events_to_print}")
						
						
	except KeyboardInterrupt:
		et = time.time()
		print(f"Time elapsed: {round(et - st, 2)} sec\n"\
			f"Events received: {event_counter}")
		time.sleep(1)
		sys.exit("Keyboard interrupt")

	