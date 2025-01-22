import string 
import subprocess
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
import time, json, os

from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING, \
    OFFSET_END


def purge_topic_via_configuration(topic=str, config_server_and_port=str):
    '''
    Configures the topic and the broker
    in order to delete the messages in the topic
    
    ``topic``: the topic whose messages we want to delete
    ``config_server_and_port``: the server and port of the kafka cluster (e.g. localhost:42880)
    '''
    
    # Move to the directory containing /bin with the kafka commands
    cd_command = "cd ../.."
    
    # Configure the topic
    topic_configs_list = ["cleanup.policy=delete", "delete.retention.ms=1", \
        "local.retention.bytes=1", "local.retention.ms=1", "retention.bytes=1", "retention.ms=1", 
        "segment.bytes=1000", "segment.ms=1"]
    
    topic_configs_formatted  = ",".join(topic_configs_list)
    
    topic_configs_command = f"bin/kafka-configs  \
        --bootstrap-server {config_server_and_port} \
        --entity-type topics  --alter --entity-name {topic}  \
        --add-config \
        {topic_configs_formatted}"
        
    
    # Configure the broker
    broker_configs_list = ["log.roll.ms=1000"]
    
    broker_configs_formatted = ",".join(broker_configs_list)
    
    broker_configs_command = f"kafka-configs \
        --bootstrap-server {config_server_and_port} \
        --entity-type brokers \
        --entity-name 1 \
        --alter --add-config \
        {broker_configs_formatted}"
    
    # Serialize the commands to execute consucutively
    docker_commands_to_execute = "; ".join([cd_command, broker_configs_command, \
                                            topic_configs_command])
    
    # Docker command to get into the container and execute 
    # the aforementioned commands
    final_docker_command = f"docker exec -i confluent-local-broker-1 bash -c \
        '{docker_commands_to_execute}'"
    
    # Run commands in terminal
    subprocess.run(final_docker_command, shell=True)


def restore_default_configs_of_topic(topic=str, config_server_and_port=str):
    
    # Move to the directory containing /bin with the kafka commands
    cd_command = "cd ../.."
    
    topic_configs_list = ["cleanup.policy", "delete.retention.ms", "local.retention.bytes", \
        "local.retention.ms", "retention.bytes", "retention.ms", "segment.bytes",\
        "segment.ms"]
    
    topic_configs_formatted  = ",".join(topic_configs_list)
    
    # Configure the topic
    topic_configs_command = f"bin/kafka-configs \
        --bootstrap-server {config_server_and_port} \
        --entity-type topics --entity-name {topic} \
        --alter --delete-config \
        {topic_configs_formatted}"
    
    
    # Configure the broker
    broker_configs_list = ["log.roll.ms"]
    
    broker_configs_formatted  = ",".join(broker_configs_list)
    
    broker_configs_command = f"kafka-configs \
        --bootstrap-server {config_server_and_port} \
        --entity-type brokers \
        --entity-name 1 \
        --alter --delete-config \
        {broker_configs_formatted}"
    
    # Serialize the commands to execute consucutively
    docker_commands_to_execute = "; ".join([cd_command, broker_configs_command, \
                                            topic_configs_command])
    
    # Docker command to get into the container and execute 
    # the aforementioned commands
    final_docker_command = f"docker exec -i confluent-local-broker-1 bash -c\
        '{docker_commands_to_execute}'"
    
    # Run commands in terminal
    subprocess.run(final_docker_command, shell=True)
    
    
def is_topic_empty(topic_name=str):
    
    
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()


    # Parse the configuration to setup the consumer
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default_consumer'])
    config.update(config_parser['raw_events_to_repos_consumer'])


    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)
                
    # Initialize consumer
    consumer = Consumer(config)

    # # Define the topic and partition
    topic_partition = TopicPartition(topic_name, partition=0)
    
    # topic_partition = TopicPartition(topic_name)
        
    # Assign the topic partition to the consumer
    consumer.assign([topic_partition])

    # Get the offsets
    my_offsets = consumer.get_watermark_offsets(topic_partition)

    consumer.close()
    
    if my_offsets[0] == my_offsets[1]:
        return True
    else:
        return False   
    

def create_config_file_to_delete_messages_of_topic(topic_name, folderpath):
    """
    Creates a .json configuration file to delete the messages of a topic
    
    ``topic_name``: The topic name
    ``folderpath``: The folder to store the configuration .json file 
    to delete the topic's messages
    
    returns: The filepath of the created configuration file 
    """
    
    # Create filename
    filename = f"delete_messages_of_topic_{topic_name}.json"
    
    # Create filepath
    delete_messages_configs_filepath = os.path.join(folderpath, filename)
    
    
    with open(delete_messages_configs_filepath, 'w') as config_file:
        configs_to_delete_messages_of_topic_dict = \
            {"partitions":[{"topic": f"{topic_name}","partition": 0,"offset": -1}],\
                "version": 1}
        json.dump(configs_to_delete_messages_of_topic_dict, config_file)
        
    
    return delete_messages_configs_filepath

def purge_topic_via_command(topic=str, config_server_and_port=str, \
    out_of_docker_config_folderpath=str):
    """
    Purges a topic using the command kafka-delete-records.sh
    The command takes a json as argument containing the topic name
    
    ``topic``: Topic whose messages are to be deleted
    ``config_server_and_port``: Host and port (e.g. localhost:34760) of the kafka 
    broker hosting the topic
    ``out_of_docker_config_filepath``: Folderpath to the configuration file that deletes the 
    messages of the topic
    
    returns: nothing
    """

    # Create the configuration file to delete its 
    # messages (the config file contains the topic's name) 
    config_filepath = create_config_file_to_delete_messages_of_topic(topic, \
        out_of_docker_config_folderpath)
    
    
    # Command to mount the 'delete messages configuration file' into the docker container
    config_filename = os.path.basename(config_filepath)
    copy_delete_messages_config_command = \
        f"docker cp {config_filepath} \
        confluent-local-broker-1:/home/{config_filename}"

  
    # Execute the command
    subprocess.run(copy_delete_messages_config_command, shell=True)
    
    # Move to the directory containing /bin with the kafka commands
    cd_command = "cd ../.."

    # Purge messages command
    purge_topic_command = f"kafka-delete-records --bootstrap-server\
        {config_server_and_port} --offset-json-file \
            /home/{config_filename}"
    
    
    # Serialize the commands to execute consecutively
    docker_commands_to_execute = "; ".join([cd_command, purge_topic_command])
    
    # Docker command to get into the container and execute 
    # the aforementioned commands
    final_docker_command = f"docker exec -i confluent-local-broker-1 bash -c\
        '{docker_commands_to_execute}'"
  
    # Run commands in terminal
    subprocess.run(final_docker_command, shell=True)
    
    
