
from argparse import ArgumentParser, FileType
from configparser import ConfigParser


from confluent_kafka import Producer, admin
from confluent_kafka.admin._config import ConfigResource, ConfigEntry,\
    ConfigSource, AlterConfigOpType

# Parse the command line.
parser = ArgumentParser()
parser.add_argument('config_file', type=FileType('r'))
args = parser.parse_args()

# Parse the configuration.
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
config_parser = ConfigParser()
config_parser.read_file(args.config_file)
config = dict(config_parser['default'])

# config_port = str(config['bootstrap.servers'])

topic = 'raw-events'


# Temporarily make the topic retention time one second.

# my_config_resource = ConfigResource(restype=ConfigResource.Type.TOPIC, \
#     name='raw-events', incremental_configs= \
#         [ConfigEntry(name='retention.ms', value='1000',\
#             source=ConfigSource.DYNAMIC_TOPIC_CONFIG,
#             incremental_operation=AlterConfigOpType.SET)])
# print(my_config_resource)

# client = admin.AdminClient(config)
# client.incremental_alter_configs([my_config_resource])

client = admin.AdminClient(config)

my_config_resource = ConfigResource(restype=ConfigResource.Type.TOPIC, \
    name='raw-events', incremental_configs= \
        [ConfigEntry(name='retention.ms', value='1000',\
            source=ConfigSource.DYNAMIC_TOPIC_CONFIG,
            incremental_operation=AlterConfigOpType.SET)])

client.incremental_alter_configs([my_config_resource])



# print("See if retention time became 1 sec.\n")

# producer_that_deletes_messages = Producer(config)
# producer_that_deletes_messages.purge(in_flight=True)

# # Block until the messages are sent (wait for 10000 seconds).
# producer_that_deletes_messages.poll(10000)
# # Wait for all messages in the Producer queue to be delivered
# producer_that_deletes_messages.flush()

# input("See if retention time became 1 sec.\n"
#     "Press a key to revert the retention time to its original value")

# # Revert the deletion time back to the log.retention.hours: 168
# my_config_resource = ConfigResource(restype=ConfigResource.Type.TOPIC, \
#     name='raw-events', incremental_configs= \
#         [ConfigEntry(name='retention.ms', value=None,\
#             source=ConfigSource.DYNAMIC_TOPIC_CONFIG,
#             incremental_operation=AlterConfigOpType.DELETE)])

# client.incremental_alter_configs([my_config_resource])