
from cassandra.cluster import Cluster
import sys
import random
import time


# 1) Connect to dockerized cassandra
# region
cassandra_host = 'cassandra_stelios'
cassandra_port = 9142
keyspace = 'test_near_real_time_data'
cluster = Cluster([cassandra_host],port=cassandra_port)
session = cluster.connect() 

create_keyspace = "CREATE KEYSPACE IF NOT EXISTS "\
    f"{keyspace} WITH replication = {{'class': 'SimpleStrategy', "\
    f"'replication_factor': '1'}} AND durable_writes = true;"
session.execute(create_keyspace)
# endregion


# 2) Create tables (ONE AT A TIME)
# region

try:

        # stats_table = "stats_by_day"
        # create_stats_table =  \
        #         f"CREATE TABLE IF NOT EXISTS {keyspace}.{stats_table} \
        #         (day text, commits counter, stars counter, pull_requests counter, \
        #         forks counter, PRIMARY KEY (day));"
        # session.execute(create_stats_table)


        popular_languages_table = "popular_languages_by_day"
        create_pop_langs_table = \
                f"CREATE TABLE IF NOT EXISTS {keyspace}.{popular_languages_table} \
                (day text, language text, num_of_occurences counter, PRIMARY KEY ((day), language)) WITH CLUSTERING ORDER BY (language desc);"
        session.execute(create_pop_langs_table)

        # print(f"Check if table {keyspace}.{popular_languages_table} was created")
        # sys.exit(0)
        # endregion


        # 3) Insert data iteratively to simulate real time changes
        # Execute queries
        # Print data as they are going to be printed in the 1st screen
        # region

        # update_stats =  session.prepare(f"UPDATE {keyspace}.{stats_table} \
        #             SET commits = commits + ?, stars = stars + ?, \
        #             forks = forks + ?, pull_requests = pull_requests + ? WHERE \
        #             day = ?;")
        # commits = random.randint(1, 10)
        # stars = random.randint(1, 3)
        # forks = random.randint(1, 2)
        # pull_requests = random.randint(1, 3)
        # day = "2025-06-15"
        # session.execute(update_stats, [commits, stars, forks, pull_requests, day])


        update_pop_langs = session.prepare(f"UPDATE {keyspace}.{popular_languages_table} " \
                        "SET num_of_occurences = num_of_occurences + 1 WHERE day = ? and language = ?")
        languages = ["Python", "C", "Java"]
        day = "2025-06-15"
        session.execute(update_pop_langs, 
                        [day, languages[random.randint(0, len(languages)-1)]])
        # print(f"Check if table {keyspace}.{popular_languages_table} was updated")
        # sys.exit(0)




        # select_stats_prepared_query = session.prepare(\
        #             f"SELECT day, commits, stars, forks, pull_requests "\
        #             f"FROM {keyspace}.{stats_table} WHERE day = ?")
        # stats_queried_rows = session.execute(select_stats_prepared_query, [day])            
        # print(f"Stats queried rows:\n{stats_queried_rows.all()}")


        select_langs_prepared_query = session.prepare(\
                f"SELECT day, language, num_of_occurences "\
                f"FROM {keyspace}.{popular_languages_table} WHERE day = ?")
        langs_queried_res = session.execute(select_langs_prepared_query, [day])            
        langs_queried_rows = langs_queried_res.all()
        langs_queried_rows_sorted = sorted(langs_queried_rows, key=lambda x: x.num_of_occurences, reverse=True)
        
        print(f"Languages queried rows sorted:\nDay:{day}\n"\
                "Language\tNumber of occurences")
        for i in range(len(langs_queried_rows_sorted)):
                print(f"{langs_queried_rows[i].language}\t{langs_queried_rows[i].num_of_occurences}")
        

        print("Check if langs table is sorted")
        cluster.shutdown() 
        sys.exit(0)


        cluster.shutdown() 
        # endregion

except SystemExit:
        pass
finally:
        cluster.shutdown() 
        print("Cluster was shut down ")


# Do it iteratively
# try: 
#     while True:

        # time.sleep(2)
        
# except KeyboardInterrupt:
#     pass


