docker exec -i kafka bash -c 'opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic historical-raw-events ; sleep 5;'

#  opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic historical-raw-events --partitions 1 --replication-factor 1'

# # Delete topic messages through deleting the topic
#  opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic historical-raw-events

# # Recreate the topic
#  opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic historical-raw-events --partitions 1 --replication-factor 1
