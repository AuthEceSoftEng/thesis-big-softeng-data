'''
Template:
https://nightlies.apache.org/flink/flink-docs-master/api/python/examples
/datastream/connectors.html#kafka-with-json-format

KafkaSink and KafkaSource are used instead of FlinkKafkaProducer and 
FlinkKafkaConsumer respectively since the latter are deprecated
(see https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/kafka/)
'''



from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, DataStream
from pyflink.datastream.connectors.kafka import  KafkaSink, \
    KafkaRecordSerializationSchema 
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.common.serialization import SimpleStringSchema


env = StreamExecutionEnvironment.get_execution_environment()
# env.add_jars("file:///home/xeon/flink-1.18.0/lib/flink-sql-connector-kafka-3.0.2-1.18.jar")


# Make sure that the Kafka cluster is started and the topic 'test_json_topic' is
# created before executing this job

type_info = Types.ROW([Types.STRING(), Types.INT()])
# type_info = Types.STRING()

ds = env.from_collection(
    [('Once', 1), ('upon', 2), ('a', 3), ('time', 4), ('there', 5)], \
    type_info=type_info)

serialization_schema = JsonRowSerializationSchema.Builder() \
    .with_type_info(type_info=type_info) \
    .build()


record_serializer = KafkaRecordSerializationSchema.builder() \
    .set_topic('test-json-topic') \
    .set_value_serialization_schema(serialization_schema) \
    .build()


# record_serializer = KafkaRecordSerializationSchema.builder() \
#     .set_topic('test-json-topic') \
#     .set_value_serialization_schema(SimpleStringSchema()) \
#     .build()

kafka_producer = KafkaSink.builder() \
    .set_bootstrap_servers("localhost:32967") \
    .set_record_serializer(record_serializer) \
    .build()


print("start writing data to kafka")
ds.sink_to(kafka_producer)
env.execute()

# kafka_consumer = KafkaSource \
#     .builder() \
#     .set_bootstrap_servers('localhost:8082') \
#     .set_topics('test_json_topic') \
#     .set_value_only_deserializer(SimpleStringSchema()) \
#     .build()

# ds = env.add_source(kafka_consumer)
