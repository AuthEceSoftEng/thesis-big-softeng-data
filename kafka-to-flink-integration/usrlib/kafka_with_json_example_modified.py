
'''https://nightlies.apache.org/flink/flink-docs-master/api/python/examples/datastream/connectors.html#kafka-with-json-format'''

# This file is a modification of kafka_with_json_example.py: https://nightlies.apache.org/flink/flink-docs-master/api/python/examples/datastream/connectors.html#kafka-with-json-format
# considering FlinkKafkaProducer and FlinkKafkaConsumer are deprecated and instead:
# KafkaSink and KafkaSource are used respectively
# (see https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/kafka/)


import logging
import sys

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import  KafkaSink, KafkaSource, KafkaRecordSerializationSchema 
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy


# Make sure that the Kafka cluster is started and the topic 'test_json_topic' is
# created before executing this job.
def write_to_kafka(env):
    type_info = Types.ROW([Types.INT(), Types.STRING()])
    ds = env.from_collection(
        [(1, 'hi'), (2, 'hello'), (3, 'hi'), (4, 'hello'), (5, 'hi'), (6, 'hello'), (6, 'hello')],
        type_info=type_info)

    serialization_schema = JsonRowSerializationSchema.Builder() \
        .with_type_info(type_info) \
        .build()
    
    # using KafkaSink instead of FlinkKafkaProducer as the latter is deprecated 
    record_serializer = KafkaRecordSerializationSchema.builder() \
        .set_topic('test_json_topic') \
        .set_value_serialization_schema(serialization_schema) \
        .build()
    
    kafka_producer = KafkaSink.builder() \
        .set_bootstrap_servers("localhost:8082") \
        .set_record_serializer(record_serializer) \
        .build()


    # note that the output type of ds must be RowTypeInfo
    ds.add_sink(kafka_producer).print()
    env.execute()


def read_from_kafka(env):
    
    # type_info = Types.ROW([Types.INT(), Types.STRING()])

    # # Same as the producer's (write_to_kafka) serialization schema
    # deserialization_schema = JsonRowSerializationSchema.Builder() \
    #     .with_type_info(type_info) \
    #     .build()
    
    # using KafkaSource instead of FlinkKafkaConsumer as the latter is deprecated     
    kafka_consumer = KafkaSource \
        .builder() \
        .set_bootstrap_servers('localhost:8082') \
        .set_topics('test_json_topic') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # env.from_source_source(kafka_consumer, 
    #                 watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
    #                 source_name="events_topic_source")
    
    ds = env.add_source(kafka_consumer)

    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///home/xeon/flink-1.18.0/lib/flink-sql-connector-kafka-3.0.2-1.18.jar")
    env.add_jars("file:////home/xeon//flink-1.18.0/lib/kafka-clients-3.6.1.jar")

    # env.add_jars("file///home/xeon/.local/lib/python3.8/site-packages/pyflink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar")


    print("start writing data to kafka")
    write_to_kafka(env)

    print("start reading data from kafka")
    read_from_kafka(env)