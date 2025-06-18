
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
import time
from requests_sse import EventSource, InvalidStatusCodeError, InvalidContentTypeError

import sys
import json
from collections import defaultdict
from confluent_kafka import Producer, Consumer, admin, TopicPartition, KafkaException
import requests



def create_topic_if_not_exists(topic, bootstrap_servers, desired_number_of_partitions):
    '''
    Checks if a topic exists and if it does not, it creates it
    `topic`: topic name 
    `config_port`: the port of the kafka cluster (e.g. localhost:44352)
    '''
    client = admin.AdminClient({"bootstrap.servers": bootstrap_servers})
    topic_metadata = client.list_topics(timeout=5)
    all_topics_list = topic_metadata.topics.keys()
    replication_factor = 1
                    
    # If the topic does not exist, create it    
    if topic not in all_topics_list:
        
        # Create topic
        print(f"Topic {topic} does not exist.\nCreating topic...")
        new_topic = admin.NewTopic(topic, num_partitions=desired_number_of_partitions, replication_factor=replication_factor)
        
        # Wait until topic is created
        try:
            create_topic_to_future_dict = client.create_topics([new_topic], operation_timeout=5)
            for create_topic_future in create_topic_to_future_dict.values():
                create_topic_future.result()
            
        # Handle create-topic exceptions
        except KafkaException as e:
            err_obj = e.args[0]
            err_name = err_obj.name()
            # If exists, create partitions
            if err_name == 'TOPIC_ALREADY_EXISTS':
                # Get current number of partitions
                print(f"Topic {topic} already exists and has {desired_number_of_partitions} partition(s)")
                topic_metadata = client.list_topics(timeout=5)
                # If partitions are few, increase them
                if current_number_of_partitions < desired_number_of_partitions:    
                    print(f"Increasing partitions of topic {topic} from {current_number_of_partitions} to {desired_number_of_partitions}...")
                    new_partitions = admin.NewPartitions(topic, new_total_count=desired_number_of_partitions)
                    # Wait until the number of the topic partitions is increased
                    create_partitions_futures = client.create_partitions([new_partitions])
                    for create_partition_future in create_partitions_futures.values():
                        create_partition_future.result()
                # Else, do nothing
                else:
                    print(f"Topic {topic} already exists and has {current_number_of_partitions} partitions")
                    pass
                    
            # Catch other errors
            else:
                raise Exception(e)
        print("Done")
    
    
    elif topic in all_topics_list:
        current_number_of_partitions = len(topic_metadata.topics[topic].partitions)
        if current_number_of_partitions != desired_number_of_partitions:
            new_partitions = admin.NewPartitions(topic, new_total_count=desired_number_of_partitions)
            # Wait until the number of the topic partitions is increased
            create_partitions_futures = client.create_partitions([new_partitions])
            for create_partition_future in create_partitions_futures.values():
                try:
                    create_partition_future.result()
                except KafkaException as e:
                    err_obj = e.args[0]
                    err_name = err_obj.name()
                    # If partitions are as many as they should be, do nothing
                    if err_name == 'INVALID_PARTITIONS':
                        print(f"Topic {topic} already exists and has {current_number_of_partitions} partitions")
    
    else:
        raise Exception(f"Topic {topic} neither exists or is absent from the kafka cluster")

# def thin_real_time_json_event(event):
# 	"""
# 	Function that receives as input an event and keeps only fields of interest.
# 	"""
	
# 	new_event = defaultdict(dict)
# 	try:
# 		new_event = {
# 			"type": event["type"],	
# 			"repo": {
# 	   			"full_name": event["repo"]["name"]
# 		  	},
# 			"created_at": event["created_at"]
# 		}
# 		if new_event["type"] == "PushEvent":
# 			new_event["payload"] = {
# 		   		"size": event["payload"]["size"],
# 				"distinct_size": event["payload"]["distinct_size"],
# 			}		
			
# 		elif new_event["type"] in ["WatchEvent", "ForkEvent"]:
# 			new_event["username"] = {
# 				"username": event["actor"]["login"]		
# 			}
# 		elif new_event["type"] in ["PullRequestEvent"]:
# 			new_event["payload"] = {
# 				"action": event["payload"]["action"],
# 				"language": event["payload"]["pull_request"]["base"]["repo"]["language"],
# 				"topics": event["payload"]["pull_request"]["base"]["repo"]["topics"]
# 			}
# 		elif new_event["type"] in ["PullRequestReviewEvent", \
# 	  			"PullRequestReviewCommentEvent"]:
# 			new_event["payload"] = {
# 				"language": event["payload"]["pull_request"]["base"]["repo"]["language"],
# 				"topics": event["payload"]["pull_request"]["base"]["repo"]["topics"]
# 			}
	
# 	except KeyError as e:
# 		raise KeyError(e)
  
# 	return new_event

def thin_real_time_json_event(event):
	"""
	Function that receives as input an event and keeps only fields of interest.
	"""
	
	new_event = defaultdict(dict)
	try:
		new_event["id"] = event["id"]
		new_event["type"] = event["type"]
		new_event["repo"] = {"full_name" : event["repo"]["name"]} # Field ["repo"]["name"] is the field with the full repo name in the raw events
		new_event["created_at"] = event["created_at"]
		
		if new_event["type"] == "PushEvent":
			new_event["payload"] = {
       			"size" : event["payload"]["size"],
				 "distinct_size" : event["payload"]["distinct_size"]
     		}
			
		elif new_event["type"] in ["WatchEvent", "ForkEvent"]:
			new_event["actor"] = event["actor"]["login"]		
			
		elif new_event["type"] == "PullRequestEvent":
			new_event["payload"] = {
				"action" : event["payload"]["action"], 
       			"pull_request":{"base":{"repo":
       				{"language": event["payload"]["pull_request"]["base"]["repo"]["language"],
            		"topics": event["payload"]["pull_request"]["base"]["repo"]["topics"]
              	}}}}
       				
		elif new_event["type"] in ["PullRequestReviewEvent", \
      			"PullRequestReviewCommentEvent", \
    			"PullRequestReviewThreadEvent"]:
			new_event["payload"] = {
       			"pull_request":{"base":{"repo":
       				{"language": event["payload"]["pull_request"]["base"]["repo"]["language"],
            		"topics": event["payload"]["pull_request"]["base"]["repo"]["topics"]
              	}}}}
       		
		# Typecast from defaultdict to regular dict
		new_event = dict(new_event)
    
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
	
	producer = Producer(config)
 
	config_port = str(config['bootstrap.servers'])		
	real_time_events_topic = "real-time-raw-events"
	desired_number_of_partitions =  4
	create_topic_if_not_exists(real_time_events_topic, config_port, desired_number_of_partitions)
 
	topic_that_retains_message_order = 'real-time-raw-events-ordered'
	desired_number_of_partitions =  1
	create_topic_if_not_exists(topic_that_retains_message_order, config_port, desired_number_of_partitions)
 

	try:		
		with  EventSource('http://github-firehose.libraries.io/events', timeout=30) as event_source:
			for event in event_source:
				event_counter += 1
	
				event_data = json.loads(event.data) 			
				event_data_thinned = thin_real_time_json_event(event_data)
				event_data_thinned_str = str(event_data_thinned)
						
				# print(f"Got event data No {event_counter}.: {event_data}")
    			# print(f"Got thinned event data No {event_counter}.: {event_data_thinned}")
				

				producer.produce(real_time_events_topic, value=event_data_thinned_str, callback=delivery_callback)
				
				jsonStrRetainingOrder = str({"id": event_data_thinned["id"], "created_at": event_data_thinned["created_at"]})
				producer.produce(topic_that_retains_message_order, value=jsonStrRetainingOrder, callback=delivery_callback)
                    
				producer.poll(0)
				
				sys.stdout.write("\r\033[KEvents produced: {0}\n".format(event_counter))									
				sys.stdout.flush()

				time.sleep(0.01)

				
				
	except (KeyboardInterrupt, SystemExit):
		et = time.time()
		print(f"\nTime elapsed: {round(et - st, 2)} sec\n"\
			f"Events received: {event_counter}")
		time.sleep(1)
	
	except InvalidStatusCodeError:
		pass
	except InvalidContentTypeError:
		pass
	except requests.RequestException:
		pass
    
	finally:
		producer.poll(0)
		producer.flush()
		print("Firehose producer closed successfully")

	