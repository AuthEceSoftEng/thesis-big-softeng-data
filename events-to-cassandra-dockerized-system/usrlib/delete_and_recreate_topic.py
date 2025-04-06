from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, admin, TopicPartition
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
    topic_metadata = client.list_topics()
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
    topic_metadata = client.list_topics()
    all_topics_list = topic_metadata.topics.keys()

    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'count_number_of_messages_group',
        'auto.offset.reset': 'earliest'
    })

    # If topic exists and is large sized, delete it
    if topic in all_topics_list:
        number_of_partitions = len(topic_metadata.topics[topic].partitions)
        message_count = 0
        for topic_partition_id in range(number_of_partitions):
            tp = TopicPartition(topic, topic_partition_id)
            low, high = consumer.get_watermark_offsets(tp)
            message_count = message_count + (high - low)

        if message_count >= max_number_of_messages:
            print(f"Topic size: {message_count} messages. Deleting topic...")
            client.delete_topics([topic])
            time.sleep(5)
            print("Done")

    # Get topics in broker after topic deletion
    topic_metadata = client.list_topics()
    all_topics_list = topic_metadata.topics.keys()
    number_of_partitions = 4    
    # If topic does not exist or was deleted, recreate it
    if topic not in all_topics_list:
        print("Recreating topic...")
        recreated_topic = admin.NewTopic(topic=topic, num_partitions=number_of_partitions, replication_factor=1)
        client.create_topics([recreated_topic])
        time.sleep(3)
        print("Done")

    consumer.close()


# # Demo: Delete the topic and recreate it 
# topic = 'historical-raw-events'
# bootstrap_servers = get_kafka_broker_config(topic)
# number_of_messages = get_topic_number_of_messages(topic, bootstrap_servers)
# max_number_of_messages = 10000
# delete_and_recreate_topic(topic, max_number_of_messages, bootstrap_servers)

