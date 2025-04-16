from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, admin, TopicPartition, KafkaException
import time


def get_kafka_broker_config(topic=str):   
	# Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default_producer'])
	
    config_port = str(config['bootstrap.servers'])
    return config_port

def get_topic_number_of_messages(topic, bootstrap_servers):
    client = admin.AdminClient({"bootstrap.servers": bootstrap_servers})
    topic_metadata = client.list_topics(timeout=5)
    all_topics_list = topic_metadata.topics.keys()
    
    if topic in all_topics_list:
        # Get partitions of the topic
        number_of_partitions = len(topic_metadata.topics[topic].partitions)
        message_count = 0
        consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'count_number_of_messages_group',
            'auto.offset.reset': 'earliest'
        })
        # Sum the number of messages in each partition
        for topic_partition_id in range(number_of_partitions):
            tp = TopicPartition(topic, topic_partition_id)
            low, high = consumer.get_watermark_offsets(tp)
            message_count = message_count + (high - low)
        consumer.close()
        return message_count
    else:
        print(f"Cannot get topic {topic}'s number of messages. The topic does not exist")
        return None

def delete_and_recreate_topic(topic, max_number_of_messages, bootstrap_servers):
    """
    If the number of messages in the ``topic`` exceeds ``max_number_of_messages``, delete and recreate it 
    """
    client = admin.AdminClient({"bootstrap.servers": bootstrap_servers})
    topic_metadata = client.list_topics(timeout=5)
    all_topics_list = topic_metadata.topics.keys()
    number_of_partitions = 4
    replication_factor = 1

    # If topic exists and has too many messages, delete it or discard its messages
    if topic in all_topics_list:
        message_count = get_topic_number_of_messages(topic, bootstrap_servers)
        if message_count >= max_number_of_messages:
            print(f"Number of messages in topic {topic} ({message_count}) is equal to or exceeds max ({max_number_of_messages}). Deleting topic...")
            
            # Wait until the topic is deleted
            delete_topic_to_future_dict = client.delete_topics([topic])
            for delete_topic_future in delete_topic_to_future_dict.values():
                delete_topic_future.result()
            print("Done")
    else:
        raise Exception(f"Cannot delete and recreate topic {topic} as it does not exist")    
    
    
    # Get kafka topics after deletion
    list_topics_futures = client.list_topics(timeout=5)
    all_topics_list = list_topics_futures.topics.keys()
    # print(all_topics_list)
    
    # If the topic was deleted, recreate it    
    if topic not in all_topics_list:
        print("Recreating topic...")
        recreated_topic = admin.NewTopic(topic, num_partitions=number_of_partitions, replication_factor=replication_factor)
        # Wait until topic is created
        create_topic_to_future_dict = client.create_topics([recreated_topic], operation_timeout=5)
        for create_topic_future in create_topic_to_future_dict.values():
            try:
                create_topic_future.result()
            except KafkaException as e:
                err_obj = e.args[0]
                err_name = err_obj.name()
                if err_name == 'TOPIC_ALREADY_EXISTS':
                    print(f"Topic {topic} already exists and has {number_of_partitions} partitions")
                    topic_metadata = client.list_topics(timeout=5)
                    all_topics_list = topic_metadata.topics.keys()
                    current_number_of_partitions = len(topic_metadata.topics[topic].partitions)
                    if current_number_of_partitions != number_of_partitions:    
                        print(f"Increasing partitions of topic {topic} from {current_number_of_partitions} to {number_of_partitions}...")
                        new_partitions = admin.NewPartitions(topic, new_total_count=number_of_partitions)
                        # Wait until the number of the topic partitions is increased
                        create_partitions_futures = client.create_partitions([new_partitions])
                        for create_partition_future in create_partitions_futures.values():
                            create_partition_future.result()
                else:
                    # Catch other errors
                    raise Exception(e)
        print("Done")
    
    
        
    # If only the topic messages were deleted, increase the topic's partitions to 4
    elif (topic in all_topics_list) and get_topic_number_of_messages(topic, bootstrap_servers) == 0:
        new_partitions = admin.NewPartitions(topic, new_total_count=number_of_partitions)
        # Wait until the number of the topic partitions is increased
        create_partitions_futures = client.create_partitions([new_partitions])
        for create_partition_future in create_partitions_futures.values():
            create_partition_future.result()
    elif (topic in all_topics_list) and get_topic_number_of_messages(topic, bootstrap_servers) > 0:
        # Topic exists and has some messages - do nothing
        pass
    
    # Error: Topic does not exist or its messages are not 0
    else:
        raise Exception(f"Topic {topic} does not exist in kafka cluster or it exists but its messages are not 0 despite having deleted it")
         
    


# # Demo: Delete the topic and recreate it 
# topic = 'historical-raw-events'
# bootstrap_servers = get_kafka_broker_config(topic)
# number_of_messages = get_topic_number_of_messages(topic, bootstrap_servers)
# max_number_of_messages = 10000
# delete_and_recreate_topic(topic, max_number_of_messages, bootstrap_servers)


