#!/bin/bash

# Delete the kafka topic before the 
docker exec kafka kafka-topics.sh --delete --topic events-topic \
--bootstrap-server kafka:9092 