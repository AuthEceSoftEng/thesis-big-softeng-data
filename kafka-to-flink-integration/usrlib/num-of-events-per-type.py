'''
Template:
https://nightlies.apache.org/flink/flink-docs-master/api/python/examples
/datastream/connectors.html#kafka-with-json-format

KafkaSink and KafkaSource are used instead of FlinkKafkaProducer and 
FlinkKafkaConsumer respectively since the latter are deprecated
(see https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/kafka/)
'''

from pyflink.common import Types, Row
from pyflink.datastream import StreamExecutionEnvironment, DataStream
from pyflink.datastream.connectors.kafka import  KafkaSink, KafkaSource, \
    KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy

import json, logging, sys

env = StreamExecutionEnvironment.get_execution_environment()
# env.add_jars("file:///home/xeon/flink-1.18.0/lib/flink-sql-connector-kafka-3.0.2-1.18.jar")


# Kafka consumer
kafka_consumer = KafkaSource.builder() \
            .set_bootstrap_servers("kafka:9092") \
            .set_topics("raw-events") \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

print("start reading data from kafka topic raw-events")
ds_raw = env.from_source( source=kafka_consumer, \
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="kafka_source") 



# Kafka producer

# # type_info = Types.ROW(Types.TUPLE([Types.STRING(), Types.INT()]))
type_info = Types.ROW_NAMED(['event_type', 'num_of_occurences'],[Types.STRING(), Types.INT()])

record_schema = JsonRowSerializationSchema.builder() \
                    .with_type_info(type_info) \
                    .build()
                     
record_serializer = KafkaRecordSerializationSchema.builder() \
    .set_topic('event-count') \
    .set_value_serialization_schema(record_schema) \
    .build()

kafka_producer = KafkaSink.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_record_serializer(record_serializer) \
    .build()

def extract_event_type(eventString):
        eventDict = eval(eventString)
        eventType = eventDict["type"]
        # eventTypeTempList = [eventType]
        # eventTypeTempTuple = eventType
        return eventType

def create_row(eventType):
        newRow = Row(eventType, 1)
        # newRow.get_fields_by_names()
        return newRow

def key_the_row(event_row):
        # print(event_row[0])
        return event_row[0]

def reduce_the_row(row_keyed_i, row_keyed_j):
        return Row(row_keyed_i[0], row_keyed_i[1] + row_keyed_j[1])

ds_count = ds_raw.map(extract_event_type, output_type=Types.STRING()) \
        .map(create_row, output_type=type_info) \
        .key_by(key_the_row, key_type=Types.STRING()) \
        .reduce(reduce_the_row)

# ds_count = ds_raw.flat_map(extract_event_type) \
#         .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
#         .key_by(lambda i: i[0]) \
#         .reduce(lambda i, j: (i[0], i[1] + j[1]))

ds_count.sink_to(kafka_producer)

ds_count.print()

env.execute()