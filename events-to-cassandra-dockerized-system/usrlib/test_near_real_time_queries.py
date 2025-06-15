
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


        # popular_languages_table = "popular_languages_by_day"
        # create_pop_langs_table = \
        #         f"CREATE TABLE IF NOT EXISTS {keyspace}.{popular_languages_table} \
        #         (day text, language text, num_of_occurences counter, PRIMARY KEY ((day), language)) WITH CLUSTERING ORDER BY (language desc);"
        # session.execute(create_pop_langs_table)

        # popular_topics_table = "popular_topics_by_day"
        # create_pop_topics_table = \
        #         f"CREATE TABLE IF NOT EXISTS {keyspace}.{popular_topics_table} \
        #         (day text, topic text, num_of_occurences counter, PRIMARY KEY ((day), topic)) WITH CLUSTERING ORDER BY (topic desc);"
        # session.execute(create_pop_topics_table)


        most_popular_repos_table = "most_popular_repos"
        create_most_popular_repos_table = \
                f"CREATE TABLE IF NOT EXISTS {keyspace}.{most_popular_repos_table} \
                (day text, repo text, stars counter, forks counter, PRIMARY KEY ((day), repo)) WITH CLUSTERING ORDER BY (repo desc);"
        session.execute(create_most_popular_repos_table)


        # print(f"Check if the table {keyspace}.{most_popular_repos_table} was created")
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


        # update_pop_langs = session.prepare(f"UPDATE {keyspace}.{popular_languages_table} " \
        #                 "SET num_of_occurences = num_of_occurences + 1 WHERE day = ? and language = ?")
        # languages = ["Python", "C", "Java"]
        # day = "2025-06-15"
        # session.execute(update_pop_langs, 
        #                 [day, languages[random.randint(0, len(languages)-1)]])
        
        
        # update_pop_topics = session.prepare(f"UPDATE {keyspace}.{popular_topics_table} " \
        #                 "SET num_of_occurences = num_of_occurences + 1 WHERE day = ? and topic = ?")
        # topics = ["hacktoberfest", "machine learning", "leet-code"]
        # day = "2025-06-15"
        # session.execute(update_pop_topics, 
        #                 [day, topics[random.randint(0, len(topics)-1)]])
        
        
        
        update_stars = session.prepare(f"UPDATE {keyspace}.{most_popular_repos_table} " \
                "SET stars = stars + 1, forks = forks + 0 WHERE day = ? and repo = ?")
        update_forks = session.prepare(f"UPDATE {keyspace}.{most_popular_repos_table} " \
                "SET forks = forks + 1, stars = stars + 0  WHERE day = ? and repo = ?")
        
        repos = ["sindresorhus/awesome", "xiangechen/chili3d", "Shubhamsaboo/awesome-llm-apps"]
        day = "2025-06-15"
        session.execute(update_stars, 
                        [day, repos[random.randint(0, len(repos)-1)]])
        session.execute(update_forks, 
                        [day, repos[random.randint(0, len(repos)-1)]])
        
        
        
        # print(f"Check if table {keyspace}.{most_popular_repos_table} was updated")
        # sys.exit(0)




        # select_stats_prepared_query = session.prepare(\
        #             f"SELECT day, commits, stars, forks, pull_requests "\
        #             f"FROM {keyspace}.{stats_table} WHERE day = ?")
        # stats_queried_rows = session.execute(select_stats_prepared_query, [day])            
        # print(f"Stats queried rows:\n{stats_queried_rows.all()}")


        # select_langs_prepared_query = session.prepare(\
        #         f"SELECT day, language, num_of_occurences "\
        #         f"FROM {keyspace}.{popular_languages_table} WHERE day = ?")
        # langs_queried_res = session.execute(select_langs_prepared_query, [day])            
        # langs_queried_rows = langs_queried_res.all()
        # langs_queried_rows_sorted = sorted(langs_queried_rows, key=lambda x: x.num_of_occurences, reverse=True)
        # print(f"Languages queried rows sorted:\nDay:{day}\n"\
        #         "Language\tNumber of occurences")
        # for i in range(len(langs_queried_rows_sorted)):
        #         print(f"{langs_queried_rows_sorted[i].language}\t{langs_queried_rows_sorted[i].num_of_occurences}")
        
        
        # select_topics_prepared_query = session.prepare(\
        #         f"SELECT day, topic, num_of_occurences "\
        #         f"FROM {keyspace}.{popular_topics_table} WHERE day = ?")
        # topics_queried_res = session.execute(select_topics_prepared_query, [day])            
        # topics_queried_rows = topics_queried_res.all()
        # topics_queried_rows_sorted = sorted(topics_queried_rows, key=lambda x: x.num_of_occurences, reverse=True)
        # print(f"Topics queried rows sorted:\nDay:{day}\n"\
        #         "Topic\tNumber of occurences")
        # for i in range(len(topics_queried_rows_sorted)):
        #         print(f"{topics_queried_rows_sorted[i].topic}\t{topics_queried_rows_sorted[i].num_of_occurences}")
        
        select_repos_prepared_query = session.prepare(\
                f"SELECT day, repo, stars, forks "\
                f"FROM {keyspace}.{most_popular_repos_table} WHERE day = ?")
        repos_queried_res = session.execute(select_repos_prepared_query, [day])            
        repos_queried_rows = repos_queried_res.all()
        
        repos_queried_rows_sorted_by_stars = sorted(repos_queried_rows, key=lambda x: x.stars, reverse=True)
        print(f"Repos queried rows sorted by stars:\nDay:{day}\n"\
                "Repo\tStars")
        for i in range(len(repos_queried_rows_sorted_by_stars)):
                print(f"{repos_queried_rows_sorted_by_stars[i].repo}\t{repos_queried_rows_sorted_by_stars[i].stars}")
        
        
        repos_queried_rows_sorted_by_forks = sorted(repos_queried_rows, key=lambda x: x.forks, reverse=True)
        print(f"Repos queried rows sorted by forks:\nDay:{day}\n"\
                "Repo\tForks")
        for i in range(len(repos_queried_rows_sorted_by_forks)):
                print(f"{repos_queried_rows_sorted_by_forks[i].repo}\t{repos_queried_rows_sorted_by_forks[i].forks}")
        
        
        print("Check if the table is sorted")
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


