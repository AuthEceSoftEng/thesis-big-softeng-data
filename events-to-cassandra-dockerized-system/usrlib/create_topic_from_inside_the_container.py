from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, admin
import subprocess


def create_topic(topic=str, config_port=str):
        '''
        Checks if a topic exists and if it does not, it creates it
        `topic`: topic name 
        `config_port`: the port of the kafka cluster (e.g. localhost:44352)
        '''
        client = admin.AdminClient({"bootstrap.servers": config_port})
        topic_metadata = client.list_topics()
        all_topics_list = topic_metadata.topics.keys()
        
        # If the topic does not exist, delete the contents of the file, 
        # then create it 
        if topic not in all_topics_list:
            print(f'Topic {topic} does not exist')
            new_topic = admin.NewTopic(topic)
            client.create_topics([new_topic])
            print(f'Topic {topic} was created')
        else: 
            print(f"Topic {topic} already exists")
            
            
            
def create_topic_from_within_container(topic=str, config_server_and_port=str):

    # Move to the directory containing /bin with the kafka commands
    cd_command = "cd ../.."

    create_topic_command = f"kafka-topics \
    --bootstrap-server {config_server_and_port} \
    --create --topic {topic}"

    # Serialize the commands to execute consucutively
    docker_commands_to_execute = "; ".join([cd_command,\
                                    create_topic_command])

    # Docker command to get into the container and execute 
    # the aforementioned commands
    final_docker_command = f"docker exec -i confluent-local-broker-1 bash -c\
    '{docker_commands_to_execute}'"

    # Run commands in terminal
    subprocess.run(final_docker_command, shell=True)
    
    
# # Demo
# # Create topic to count the events per timestamp
# num_of_events_per_timestamp_topic = 'num-of-raw-events-per-sec'
# # Parse the command line.
# parser = ArgumentParser()
# parser.add_argument('config_file', type=FileType('r'))
# args = parser.parse_args()
    
# # Parse the configuration.
# # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
# config_parser = ConfigParser()
# config_parser.read_file(args.config_file)
# config = dict(config_parser['default_producer'])

# config_port = str(config['bootstrap.servers'])

# # create_topic(num_of_events_per_timestamp_topic, config_port)

# create_topic_from_within_container(num_of_events_per_timestamp_topic, config_port)