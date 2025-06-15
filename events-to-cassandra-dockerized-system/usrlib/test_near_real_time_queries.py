
from cassandra.cluster import Cluster
import sys
import random
import time

# Connect to dockerized cassandra
cassandra_host = 'cassandra_stelios'
cassandra_port = 9142
keyspace = 'test_near_real_time_data'
cluster = Cluster([cassandra_host],port=cassandra_port)
session = cluster.connect() 

create_keyspace = "CREATE KEYSPACE IF NOT EXISTS "\
    f"{keyspace} WITH replication = {{'class': 'SimpleStrategy', "\
    f"'replication_factor': '1'}} AND durable_writes = true;"
session.execute(create_keyspace)


# Create tables (ONE AT A TIME)
stats_table = "stats_by_day"
create_stats_table =  \
        f"CREATE TABLE IF NOT EXISTS {keyspace}.{stats_table} \
        (day text, commits counter, stars counter, pull_requests counter, \
        forks counter, PRIMARY KEY (day));"
session.execute(create_stats_table)


# Insert data iteratively to simulate real time changes
# Execute queries
# Print data as they are going to be printed in the 1st screen
commits = random.randint(1, 10)
stars = random.randint(1, 3)
forks = random.randint(1, 2)
pull_requests = random.randint(1, 3)
day = "2025-06-15"
update_stats =  session.prepare(f"UPDATE {keyspace}.{stats_table} \
            SET commits = commits + ?, stars = stars + ?, \
            forks = forks + ?, pull_requests = pull_requests + ? WHERE \
            day = ?;")
session.execute(update_stats, [commits, stars, forks, pull_requests, day])




select_stats_prepared_query = session.prepare(\
            f"SELECT day, commits, stars, forks, pull_requests "\
            f"from {keyspace}.{stats_table} WHERE day = ?")
stats_queried_rows = session.execute(select_stats_prepared_query, [day])            
print(f"Stats queried rows:\n{stats_queried_rows.all()}")

print(f"Check if stats table {keyspace}.{stats_table} were updated")
sys.exit(0)
 
 




# Do it iteratively
# try: 
#     while True:

        # time.sleep(2)
        
# except KeyboardInterrupt:
#     pass


