import string 
import subprocess


port = 44731
comm2 = "cd ../.."
# comm3 = f"bin/kafka-configs  --bootstrap-server localhost:{port} --entity-type topics  --alter --entity-name raw-events  --add-config retention.ms=1000"
comm4 = f"bin/kafka-configs --bootstrap-server localhost:{port} \
    --entity-type topics --alter --entity-name raw-events \
    --delete-config retention.ms,delete.retention.ms,cleanup.policy"
docker_commands = "; ".join([comm2, comm4])
# print(docker_commands)
comm1 = f"docker exec -i confluent-local-broker-1 bash -c '{docker_commands}'"
subprocess.run(comm1, shell=True)