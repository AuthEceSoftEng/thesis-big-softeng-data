
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
import time
from requests_sse import EventSource
import sys
import json
from collections import defaultdict
from confluent_kafka import Producer
from delete_and_recreate_topic import create_topic_if_not_exists



def thin_near_real_time_json_event(event):
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

def delivery_callback(err, msg):
    '''Optional per-message delivery callback (triggered by poll() or flush())
    when a message has been successfully delivered or permanently
    failed delivery (after retries).
    '''
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        pass

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
		max_num_of_events_to_print = 1000
		st = time.time()

		# Get kafka_host:kafka_port
		parser = ArgumentParser()
		parser.add_argument('config_file', type=FileType('r'))
		args = parser.parse_args()
		config_parser = ConfigParser()
		config_parser.read_file(args.config_file)
		config = dict(config_parser['default_producer'])
		config_port = str(config['bootstrap.servers'])
            
		producer = Producer(config)
		near_real_time_events_topic = "near-real-time-raw-events"
		create_topic_if_not_exists(near_real_time_events_topic, config_port)

		with  EventSource('http://github-firehose.libraries.io/events', timeout=30) as event_source:
			for event in event_source:
				event_counter += 1
				event_data = json.loads(event.data) 
				event_data_thinned = thin_near_real_time_json_event(event_data)
				event_data_thinned_str = str(event_data_thinned)
				# print(f"Got thinned event data No {event_counter}.: {event_data_thinned}")
				# print(f"Type of thinned event data: {type(event_data_thinned)}")

				producer.produce(near_real_time_events_topic, value=event_data_thinned_str, callback=delivery_callback)
				producer.poll(0)
				
				# sys.stdout.write("\rEvents produced: {0}".format(event_counter))
				if event_counter % 100 == 0:
					sys.stdout.write("\r\033[KEvents produced: {0}\n".format(event_counter))									
					sys.stdout.flush()


				if event_counter >= max_num_of_events_to_print:
					sys.exit(f"Reached max number of events to print: "\
						f"{max_num_of_events_to_print}")
				
	except KeyboardInterrupt:
		et = time.time()
		print(f"\nTime elapsed: {round(et - st, 2)} sec\n"\
			f"Events received: {event_counter}")
		time.sleep(1)
  
		producer.poll(0)
		producer.flush()

		sys.exit("Keyboard interrupt")

	producer.poll(0)
	producer.flush()

	