# Integrate Kafka and Flink

The guide below describes how to run Kafka and Flink on docker. 

Build the docker image
```sh
docker build -f Dockerfile --tag pyflink:latest .
```

Execute the bash script below to setup for the compose of the kafka and kafka-ui services
```sh
sudo ./helpers/setup-kafka-and-ui.sh
```

Compose the services
```sh
docker compose up
```

## Issue faced
### Terminal 2
Compose the flink job kafka_with_json_example_modified.py
```sh
docker exec jobmanager-1 ./bin/flink run -py /opt/flink/usrlib/kafka_with_json_example_modified.py --jarfile /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar 
```

Error: 
```sh
WARNING: Unknown module: jdk.compiler specified to --add-exports
WARNING: Unknown module: jdk.compiler specified to --add-exports
WARNING: Unknown module: jdk.compiler specified to --add-exports
WARNING: Unknown module: jdk.compiler specified to --add-exports
WARNING: Unknown module: jdk.compiler specified to --add-exports
start writing data to kafka
Traceback (most recent call last):
  File "/opt/flink/usrlib/kafka_with_json_example_modified.py", line 87, in <module>
    write_to_kafka(env)
  File "/opt/flink/usrlib/kafka_with_json_example_modified.py", line 46, in write_to_kafka
    ds.add_sink(kafka_producer).print()
  File "/opt/flink/opt/python/pyflink.zip/pyflink/datastream/data_stream.py", line 819, in add_sink
  File "/opt/flink/opt/python/py4j-0.10.9.7-src.zip/py4j/java_gateway.py", line 1322, in __call__
  File "/opt/flink/opt/python/pyflink.zip/pyflink/util/exceptions.py", line 146, in deco
  File "/opt/flink/opt/python/py4j-0.10.9.7-src.zip/py4j/protocol.py", line 330, in get_return_value
py4j.protocol.Py4JError: An error occurred while calling o67.addSink. Trace:
org.apache.flink.api.python.shaded.py4j.Py4JException: Method addSink([class org.apache.flink.connector.kafka.sink.KafkaSink]) does not exist
        at org.apache.flink.api.python.shaded.py4j.reflection.ReflectionEngine.getMethod(ReflectionEngine.java:321)
        at org.apache.flink.api.python.shaded.py4j.reflection.ReflectionEngine.getMethod(ReflectionEngine.java:329)
        at org.apache.flink.api.python.shaded.py4j.Gateway.invoke(Gateway.java:274)
        at org.apache.flink.api.python.shaded.py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
        at org.apache.flink.api.python.shaded.py4j.commands.CallCommand.execute(CallCommand.java:79)
        at org.apache.flink.api.python.shaded.py4j.GatewayConnection.run(GatewayConnection.java:238)
        at java.base/java.lang.Thread.run(Unknown Source)


org.apache.flink.client.program.ProgramAbortException: java.lang.RuntimeException: Python process exits with code: 1
        at org.apache.flink.client.python.PythonDriver.main(PythonDriver.java:140)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(Unknown Source)
        at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(Unknown Source)
        at java.base/java.lang.reflect.Method.invoke(Unknown Source)
        at org.apache.flink.client.program.PackagedProgram.callMainMethod(PackagedProgram.java:355)
        at org.apache.flink.client.program.PackagedProgram.invokeInteractiveModeForExecution(PackagedProgram.java:222)
        at org.apache.flink.client.ClientUtils.executeProgram(ClientUtils.java:105)
        at org.apache.flink.client.cli.CliFrontend.executeProgram(CliFrontend.java:851)
        at org.apache.flink.client.cli.CliFrontend.run(CliFrontend.java:245)
        at org.apache.flink.client.cli.CliFrontend.parseAndRun(CliFrontend.java:1095)
        at org.apache.flink.client.cli.CliFrontend.lambda$mainInternal$9(CliFrontend.java:1189)
        at org.apache.flink.runtime.security.contexts.NoOpSecurityContext.runSecured(NoOpSecurityContext.java:28)
        at org.apache.flink.client.cli.CliFrontend.mainInternal(CliFrontend.java:1189)
        at org.apache.flink.client.cli.CliFrontend.main(CliFrontend.java:1157)
Caused by: java.lang.RuntimeException: Python process exits with code: 1
        at org.apache.flink.client.python.PythonDriver.main(PythonDriver.java:130)
        ... 14 more```