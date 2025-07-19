#!/bin/bash

docker exec -i kafka bash -c 'opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic historical-raw-events ; sleep 5; opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic historical-raw-events --partitions 1 --replication-factor 1
'

# # Three commands are executed consecutively:
# # Delete topic messages through deleting the topic
#  opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic historical-raw-events

# # Wait for 5 seconds for the topic deletion topics
# sleep 5

# # Recreate kafka topic
#  opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic historical-raw-events --partitions 1 --replication-factor 1'

