# Template: https://github.com/app-generator/flask-material-dashboard

from typing import OrderedDict
from flask import Flask, jsonify
from flask_cors import CORS
from cassandra.cluster import Cluster
from datetime import datetime, timedelta

from sqlalchemy import null

from dateutil.relativedelta import relativedelta

from urllib.parse import unquote
import numpy as np
import time 
from collections import OrderedDict

import sys


app = Flask(__name__)
# To return sorted dictionaries
app.config['JSON_SORT_KEYS'] = False
app.config['JSON_AS_ASCII'] = False
app.url_map.strict_slashes = False
CORS(app)


def query_distinct_results(cassandra_session, select_query=str, number_of_results=int, 
                distinct_field_name=str, initial_query_limit=5, 
                query_limit_verbose=False):
    r"""
    Description
    Given a SELECT query with ORDER BY representing a table with 
    PRIMARY KEY:  ((partition_key), clustering_key_0, clustering_key_1)
    and clustering_key_1
    having the same value across returned rows, return rows with distinct values on 
    the specific field
    
    :param query: The query to get the response from. Should be in the form of
    
    >>> Example SELECT * FROM {keyspace}.popular_topics_by_day \
        WHERE day = '2024-09-09' \
        ORDER BY number_of_events DESC LIMIT {query_limit};
    
    :param cassandra_session: The session returned from a cassandra cluster 
    connection. A keyspace for the      
    :param number_of_results: The number of distinct 
    results to return from the query
    :param distinct_field_name: The field name 
    we need distinct results on. 
    :param initial_query_limit: The initial limit to query the results. 
    If the number of results requested are not found, it is doubled and 
    the query is performed again
    :param query_limit_verbose: If True, print the query limit on increase
    
    :return distinct_row_list: rows of a table with DISTINCT clustering_key_1
    :return None: if the returned list is empty
    
    Example: returned query values:
    
    - query response:
     day        | number_of_events | topic
    ------------+------------------+---------------
    2024-09-09 |              683 |    typescript
    2024-09-09 |              682 | hacktoberfest
    2024-09-09 |              682 |        python
    2024-09-09 |              682 |    typescript
    2024-09-09 |              681 | hacktoberfest
    2024-09-09 |              681 |        python
    
    - number_of_results we want to keep: 3
    
    Then the returned table will be a list of three elements each
    being a list of the topic 
    
    [[typescript, 683], [hacktoberfest, 682], [python, 682]]
    
    """
    
    near_real_time_keyspace = 'near_real_time_gharchive'    
    cluster = Cluster(['cassandra_stelios'], port=9142)
    session = cluster.connect(f'{near_real_time_keyspace}')

    # Get the field index returned by the query corresponding to the 
    # `distinct_field_name`
    res_result_set_to_get_fields = \
        session.execute(select_query.rsplit('LIMIT')[0] + "LIMIT 1")
    if res_result_set_to_get_fields == []:
        return []
    
    distinct_field_index_in_row = \
        res_result_set_to_get_fields.one()._fields.index(distinct_field_name)

    # List with distinct values of the field `distinct_field_name` 
    distinct_field_list = []
    # Rows returned with the distinct values for the field `distinct_field_name`
    distinct_row_list = []


    # Query repeatedly on a higher query limit each time to get many 
    # distinct results
    query_limit = initial_query_limit
    while (len(distinct_field_list) < number_of_results):
        
        # Increase the query limit 
        query_limit = 2*query_limit
        if query_limit_verbose == True:
            print(f"Increasing the query limit to {query_limit}")
        
        # Query on the new query limits
        select_query = select_query.rsplit('LIMIT')[0] + f"LIMIT {query_limit};"
        res_result_set = cassandra_session.execute(select_query)
        # List with the new resulting rows 
        res_rows_list = res_result_set.all()
        

        # Find the distinct rows 
        for res_row in res_rows_list:
            if res_row[distinct_field_index_in_row] not in distinct_field_list:
                distinct_field_list.append(res_row[distinct_field_index_in_row])
                distinct_row_list.append(res_row)
                if len(distinct_field_list) == number_of_results:
                    break

        # Print message and break if there is not a `distinct_number_of_results`
        # to bring from the queried table 
        # (or equivalently if the query_limit exceeds the number 
        # of returned table rows)
        if query_limit > len(res_rows_list) :
            print(f"\nThe number of distinct results that was asked for "
                f"({number_of_results}) is too high "
                f"for the given cassandra table field (field index: "
                f"{distinct_field_index_in_row} with name "
                f"{res_rows_list[0]._fields[1]})\n"
                f"Only {len(distinct_field_list)} found")
            break
    return distinct_row_list 
    # print(distinct_field_list)


number_of_results = 10
initial_query_limit = 10


# Screen 1: Near real time events
# region
# Expose stats_by_day for Q1

@app.route('/stats_by_day', methods=['GET'])
def get_stats_of_day():
    
    near_real_time_keyspace = 'near_real_time_gharchive'    
    cluster = Cluster(['cassandra_stelios'], port=9142)
    session = cluster.connect(f'{near_real_time_keyspace}')

    # Figure the latest date for which data is available (either today or yesterday)
    latest_date_available = datetime.now().strftime("%Y-%m-%d")
    stats_select_query = f" \
    SELECT day, SUM(stars) AS total_stars, SUM(forks) AS total_forks, \
        SUM(commits) AS total_commits, SUM(pull_requests) AS total_pull_requests \
        FROM {near_real_time_keyspace}.stats_by_day \
        WHERE day = '{latest_date_available}';\
    "
    # Query to figure out the latest day for which data is available
    rows = session.execute(stats_select_query)
   
    # Change the latest date available to yesterday and make the query on that day
    if rows.one().day == None:
        latest_date_available = datetime.now() - timedelta(days=1)    
        latest_date_available = latest_date_available.strftime("%Y-%m-%d")
        stats_select_query = f" \
        SELECT day, SUM(stars) AS total_stars, SUM(forks) AS total_forks, \
            SUM(commits) AS total_commits, SUM(pull_requests) AS total_pull_requests \
            FROM {near_real_time_keyspace}.stats_by_day \
            WHERE day = '{latest_date_available}';\
        "
        # Perform the query again should there be no queried rows from today
        rows = session.execute(stats_select_query)

    # return(jsonify(stats_select_query))
    
    cluster.shutdown()
    result = []
    # Create a JSON-serializable object from the resulting data
    for row in rows:
        result.append({db_col_name: getattr(row, db_col_name) for db_col_name in row._fields})
            
    return jsonify(result)


# Expose most_starred_repos_by_day for Q2
@app.route('/most_starred_repos_by_day', methods=['GET'])
def get_most_starred_repos_by_day():
    distinct_field_name = 'repo_name'
    near_real_time_keyspace = 'near_real_time_gharchive'    
    # Figure the latest date for which data is available (either today or yesterday)
    latest_date_available = datetime.now().strftime("%Y-%m-%d")
    topic_select_query = f" \
    SELECT day, repo_name, stars FROM {near_real_time_keyspace}.stats_by_day \
        WHERE day = '{latest_date_available}' \
        ORDER BY stars DESC LIMIT 1;\
    "
    cluster = Cluster(['cassandra_stelios'], port=9142)
    session = cluster.connect(f'{near_real_time_keyspace}')

    # Query to figure out the latest day for which data is available
    rows = query_distinct_results(session, topic_select_query, number_of_results, \
        distinct_field_name, initial_query_limit)



    # Change the latest date available to yesterday and make the query on that day
    if rows == []:
        latest_date_available = datetime.now() - timedelta(days=1)    
        latest_date_available = latest_date_available.strftime("%Y-%m-%d")
        topic_select_query = f" \
        SELECT day, repo_name, stars FROM {near_real_time_keyspace}.stats_by_day \
            WHERE day = '{latest_date_available}' \
            ORDER BY stars DESC LIMIT 1;\
        "
        # Perform the query again should there be no queried rows from today
        rows = query_distinct_results(session, topic_select_query, number_of_results, \
        distinct_field_name, initial_query_limit)
    
    cluster.shutdown()
    result = []
    # Create a JSON-serializable object from the resulting data
    for row in rows:
        result.append({db_col_name: getattr(row, db_col_name) for db_col_name in row._fields})
            
    return jsonify(result)

# Expose most_forked_repos_by_day for Q3
@app.route('/most_forked_repos_by_day', methods=['GET'])
def get_most_forked_repos_by_day():
    distinct_field_name = 'repo_name'

    near_real_time_keyspace = 'near_real_time_gharchive'    
    cluster = Cluster(['cassandra_stelios'], port=9142)
    session = cluster.connect(f'{near_real_time_keyspace}')

    # Figure the latest date for which data is available (either today or yesterday)
    latest_date_available = datetime.now().strftime("%Y-%m-%d")
    topic_select_query = f" \
    SELECT * FROM {near_real_time_keyspace}.forks_by_day \
        WHERE day = '{latest_date_available}' \
        ORDER BY forks DESC LIMIT 1;\
    "
    # Query to figure out the latest day for which data is available
    rows = query_distinct_results(session, topic_select_query, number_of_results, \
        distinct_field_name, initial_query_limit)



    # Change the latest date available to yesterday and make the query on that day
    if rows == []:
        latest_date_available = datetime.now() - timedelta(days=1)    
        latest_date_available = latest_date_available.strftime("%Y-%m-%d")
        topic_select_query = f" \
        SELECT * FROM {near_real_time_keyspace}.forks_by_day \
            WHERE day = '{latest_date_available}' \
            ORDER BY forks DESC LIMIT 1;\
        "
        # Perform the query again should there be no queried rows from today
        rows = query_distinct_results(session, topic_select_query, number_of_results, \
        distinct_field_name, initial_query_limit)
    
    cluster.shutdown()
    result = []
    # Create a JSON-serializable object from the resulting data
    for row in rows:
        result.append({db_col_name: getattr(row, db_col_name) for db_col_name in row._fields})
            
    return jsonify(result)


# Expose popular_languages_by_day for Q4
@app.route('/popular_languages_by_day', methods=['GET'])
def get_languages_by_day():
    distinct_field_name = 'language'
    
    near_real_time_keyspace = 'near_real_time_gharchive'    
    cluster = Cluster(['cassandra_stelios'], port=9142)
    session = cluster.connect(f'{near_real_time_keyspace}')

    # Figure the latest date for which data is available (either today or yesterday)
    latest_date_available = datetime.now().strftime("%Y-%m-%d")
    topic_select_query = f" \
    SELECT * FROM {near_real_time_keyspace}.popular_languages_by_day \
        WHERE day = '{latest_date_available}' \
        ORDER BY number_of_events DESC LIMIT 1;\
    "
    # Query to figure out the latest day for which data is available
    rows = query_distinct_results(session, topic_select_query, number_of_results, \
        distinct_field_name, initial_query_limit)



    # Change the latest date available to yesterday and make the query on that day
    
    if rows == []:
        latest_date_available = datetime.now() - timedelta(days=1)    
        latest_date_available = latest_date_available.strftime("%Y-%m-%d")
        topic_select_query = f" \
        SELECT * FROM {near_real_time_keyspace}.popular_languages_by_day \
            WHERE day = '{latest_date_available}' \
            ORDER BY number_of_events DESC LIMIT 1;\
        "
        # Perform the query again should there be no queried rows from today
        rows = query_distinct_results(session, topic_select_query, number_of_results, \
        distinct_field_name, initial_query_limit)
        
    result = []
    # Create a JSON-serializable object from the resulting data
    for row in rows:
        result.append({db_col_name: getattr(row, db_col_name) for db_col_name in row._fields})
    cluster.shutdown()
    return jsonify(result)

# Expose popular_topics_by_day for Q5
@app.route('/popular_topics_by_day', methods=['GET'])
def get_topics_by_day():
    distinct_field_name = 'topic'
    near_real_time_keyspace = 'near_real_time_gharchive'    
    cluster = Cluster(['cassandra_stelios'], port=9142)
    session = cluster.connect(f'{near_real_time_keyspace}')

    # Figure the latest date for which data is available (either today or yesterday)
    latest_date_available = datetime.now().strftime("%Y-%m-%d")
    topic_select_query = f" \
    SELECT * FROM {near_real_time_keyspace}.popular_topics_by_day \
        WHERE day = '{latest_date_available}' \
        ORDER BY number_of_events DESC LIMIT 1;\
    "
    # Query to figure out the latest day for which data is available
    rows = query_distinct_results(session, topic_select_query, number_of_results, \
        distinct_field_name, initial_query_limit)



    # Change the latest date available to yesterday and make the query on that day
    if rows == []:
        latest_date_available = datetime.now() - timedelta(days=1)    
        latest_date_available = latest_date_available.strftime("%Y-%m-%d")
        topic_select_query = f" \
        SELECT * FROM {near_real_time_keyspace}.popular_topics_by_day \
            WHERE day = '{latest_date_available}' \
            ORDER BY number_of_events DESC LIMIT 1;\
        "
        # Perform the query again should there be no queried rows from today
        rows = query_distinct_results(session, topic_select_query, number_of_results, \
        distinct_field_name, initial_query_limit)
    cluster.shutdown()
    result = []
    # Create a JSON-serializable object from the resulting data
    for row in rows:
        result.append({db_col_name: getattr(row, db_col_name) for db_col_name in row._fields})
            
    return jsonify(result)






# endregion


# Screen 2: Bots vs humans
# region

# Expose top contributing humans
@app.route('/bots_vs_humans/top_contributing_humans/<day>', methods=['GET'])
def get_top_human_contributors_by_day(day):
    
    """
    Example JSON to be exposed:
    {
        "day": "2024-03-01",
        "top_contributors": [
            {
            "number_of_contributions": 56991,
            "username": "censameesss"
            },
            {
            "number_of_contributions": 30007,
            "username": "RiskyGUY22"
            },
            {
            "number_of_contributions": 21877,
            "username": "R3CI"
            }
        ]
    }
    """
    
    # day
    day_datetime_formatted = datetime.strptime(day, "%d")
    day_only = datetime.strftime(day_datetime_formatted, "2024-12-%d")
    
    cassandra_container_name = 'cassandra_stelios'
    keyspace = 'prod_gharchive'
    cluster = Cluster([cassandra_container_name],port=9142)
    session = cluster.connect(keyspace)
    
    
    # Top contributors 
    prepared_query = f" "\
        f"SELECT username, number_of_contributions FROM {keyspace}.top_human_contributors_by_day "\
        f"WHERE day = '{day_only}';"
    
    
    
    rows = session.execute(prepared_query)
    
    # Sort the rows based on number of contributions
    dict_to_expose = {'day': day_only, 'top_contributors': []}
    
    
    result = []
    # Create a JSON-serializable object from the queried data
    for row in rows:
        result.append({db_col_name: getattr(row, db_col_name) for db_col_name in row._fields})
    
    result_sorted = sorted(result, key=lambda x: x["number_of_contributions"], reverse=True)
    # print(result_sorted)
    
    dict_to_expose["top_contributors"] = result_sorted[0:5]    
    
    cluster.shutdown()
    return jsonify(dict_to_expose)


# Expose top contributing humans, cut off humans with large commits (bot like number of commits)
@app.route('/bots_vs_humans/top_contributing_humans_bot_like_committers_removed/<day>', methods=['GET'])
def get_top_human_contributors_by_day_bot_like_committers_removed(day):
    
    """
    Example JSON to be exposed:
    {
        "day": "2024-03-01",
        "top_contributors": [
            {
            "number_of_contributions": 56991,
            "username": "censameesss"
            },
            {
            "number_of_contributions": 30007,
            "username": "RiskyGUY22"
            },
            {
            "number_of_contributions": 21877,
            "username": "R3CI"
            }
        ]
    }
    """
    
    # day
    day_datetime_formatted = datetime.strptime(day, "%d")
    day_only = datetime.strftime(day_datetime_formatted, "2024-12-%d")
    
    cassandra_container_name = 'cassandra_stelios'
    keyspace = 'prod_gharchive'
    cluster = Cluster([cassandra_container_name],port=9142)
    session = cluster.connect(keyspace)
    
    
    # Top contributors 
    prepared_query = f" "\
        f"SELECT username, number_of_contributions FROM {keyspace}.top_human_contributors_by_day "\
        f"WHERE day = '{day_only}';"
    
    
    
    rows = session.execute(prepared_query)
    
    # Sort the rows based on number of contributions
    dict_to_expose = {'day': day_only, 'top_contributors': []}
    
    
    result = []
    # Create a JSON-serializable object from the queried data
    for row in rows:
        result.append({db_col_name: getattr(row, db_col_name) for db_col_name in row._fields})
    
    result_sorted = sorted(result, key=lambda x: x["number_of_contributions"], reverse=True)
    
    # Remove committers with too many commits (more than 20 per hour)
    max_num_of_commits_per_hour = 20
    result_sorted_bot_like_committers_removed = [committer_dict for committer_dict \
        in result_sorted if committer_dict["number_of_contributions"] <= max_num_of_commits_per_hour*24]
    
    num_of_committers_to_show = 5
    dict_to_expose["top_contributors"] = result_sorted_bot_like_committers_removed[0:num_of_committers_to_show]    
    
    cluster.shutdown()
    return jsonify(dict_to_expose)


# Expose top contributing bots
@app.route('/bots_vs_humans/top_contributing_bots/<day>', methods=['GET'])
def get_top_bot_contributors_by_day(day):
    """
    Example JSON to be exposed:
    {
        "day": "2024-03-01",
        "top_contributors": [
            {
            "number_of_contributions": 20423,
            "username": "github-actions[bot]"
            },
            {
            "number_of_contributions": 6569,
            "username": "pull[bot]"
            },
            {
            "number_of_contributions": 2129,
            "username": "renovate[bot]"
            }
        ]
    }
    """
    day_datetime_formatted = datetime.strptime(day, "%d")
    day_only = datetime.strftime(day_datetime_formatted, "2024-12-%d")
    
    
    cassandra_container_name = 'cassandra_stelios'
    keyspace = 'prod_gharchive'
    cluster = Cluster([cassandra_container_name],port=9142)
    session = cluster.connect(keyspace)

    
    prepared_query = f""\
        f"SELECT username, number_of_contributions FROM {keyspace}.top_bot_contributors_by_day "\
        f"WHERE day = '{day_only}';"
    
    
    # Query to figure out the latest day for which data is available
    rows = session.execute(prepared_query)
    cluster.shutdown()
    
    dict_to_expose = {'day': day_only, 'top_contributors': []}
    result = []
    # Create a JSON-serializable object from the queried data
    for row in rows:
        result.append({db_col_name: getattr(row, db_col_name) for db_col_name in row._fields})
    
    cluster.shutdown()       

    result_sorted = sorted(result, key=lambda x: x["number_of_contributions"], reverse=True)
    
    dict_to_expose["top_contributors"] = result_sorted[0:5]
    return jsonify(dict_to_expose)

# Expose pull-requests that were accepted and initiated by humans   
@app.route('/bots_vs_humans/pull_requests_by_humans/<day>', methods=['GET'])
def get_number_of_all_pull_requests_by_humans_by_day(day):
    """
    Example JSON to be exposed:
    {
        "day": "2024-03-14",
        "number_of_accepted_pull_requests": 3493,
        "number_of_rejected_pull_requests": 626
    }
    """
    cassandra_container_name = 'cassandra_stelios'
    keyspace = 'prod_gharchive'
    cluster = Cluster([cassandra_container_name],port=9142)
    session = cluster.connect(keyspace)

    # month
    day_datetime_formatted = datetime.strptime(day, "%d")
    day_only = datetime.strftime(day_datetime_formatted, "2024-12-%d")
    
    # number_of_accepted_pull_requests
    prepared_query = f""\
        f"SELECT number_of_pull_requests FROM {keyspace}.number_of_pull_requests_by_humans "\
        f"WHERE day = '{day_only}' and were_accepted = True; "
    
    
    # Query to figure out the latest day for which data is available
    rows = session.execute(prepared_query)
    if rows == []:
        dict_to_expose = {'day': day_only, 
                      "number_of_accepted_pull_requests": None,
                      "number_of_rejected_pull_requests": None}    
        return jsonify(dict_to_expose)
    
    # Only one result (one element in the rows list) is expected
    number_of_accepted_pull_requests = getattr(rows[0], "number_of_pull_requests")
    
    
    # number_of_rejected_pull_requests
    prepared_query = f""\
        f"SELECT number_of_pull_requests FROM {keyspace}.number_of_pull_requests_by_humans "\
        f"WHERE day = '{day_only}' and were_accepted = False;"
    
    # Query to figure out the latest day for which data is available
    rows = session.execute(prepared_query)
    # Only one result (one element in the rows list) is expected
    number_of_rejected_pull_requests = getattr(rows[0], "number_of_pull_requests")
    
    cluster.shutdown()       
    
    dict_to_expose = {'day': day_only, 
                      "number_of_accepted_pull_requests": number_of_accepted_pull_requests,
                      "number_of_rejected_pull_requests": number_of_rejected_pull_requests}    
    
    return jsonify(dict_to_expose)

# Expose pull-requests (accepted and rejected) that were initiated by bots
@app.route('/bots_vs_humans/pull_requests_by_bots/<day>', methods=['GET'])
def get_number_of_all_pull_requests_by_bots_by_day(day):
    """
    Example JSON to be exposed:
    {
        "day": "2024-03-10",
        "number_of_accepted_pull_requests": 434,
        "number_of_rejected_pull_requests": 166
    }
    """
    cassandra_container_name = 'cassandra_stelios'
    keyspace = 'prod_gharchive'
    cluster = Cluster([cassandra_container_name],port=9142)
    session = cluster.connect(keyspace)

    # month
    day_datetime_formatted = datetime.strptime(day, "%d")
    day_only = datetime.strftime(day_datetime_formatted, "2024-12-%d")
    
    # number_of_accepted_pull_requests
    prepared_query = f""\
        f"SELECT number_of_pull_requests FROM {keyspace}.number_of_pull_requests_by_bots  "\
        f"WHERE day = '{day_only}' and were_accepted = True;"
    
    
    rows = session.execute(prepared_query)
    if rows == []:
        dict_to_expose = {'day': day_only, 
                      "number_of_accepted_pull_requests": None,
                      "number_of_rejected_pull_requests": None}    
        return jsonify(dict_to_expose)
    
    # Only one result (one element in the rows list) is expected
    row = rows.one()
    number_of_accepted_pull_requests = getattr(row, "number_of_pull_requests")
    
    
    # number_of_rejected_pull_requests
    prepared_query = f""\
        f"SELECT number_of_pull_requests FROM {keyspace}.number_of_pull_requests_by_bots  "\
        f"WHERE day = '{day_only}' and were_accepted = False;"
    
    
    # Query to figure out the latest day for which data is available
    rows = session.execute(prepared_query)
    row = rows.one()
    # Only one result (one element in the rows list) is expected
    number_of_rejected_pull_requests = getattr(row, "number_of_pull_requests")
    
    cluster.shutdown()       
    
    dict_to_expose = {'day': day_only, 
                      "number_of_accepted_pull_requests": number_of_accepted_pull_requests,
                      "number_of_rejected_pull_requests": number_of_rejected_pull_requests}    
    
    return jsonify(dict_to_expose)

# Expose number of events made by humans and bots
@app.route('/bots_vs_humans/number_of_events_by_humans_and_bots/<day>', methods=['GET'])
def get_number_of_events_for_humans_and_bots_by_day(day):
    """
    Example JSON to be exposed:
    {
        "bots": {
            "day": "2024-06-10",
            "number_of_events_per_type": [
            {
                "event_type": "PushEvent",
                "number_of_events": 37808
            },
            {
                "event_type": "GollumEvent",
                "number_of_events": 1
            }
            ],
            "total_number_of_events": 62701
        },
        "humans": {
            "day": "2024-06-09",
            "number_of_events_per_type": [
            {
                "event_type": "PushEvent",
                "number_of_events": 104605
            },
            {
                "event_type": "CommitCommentEvent",
                "number_of_events": 125
            }
            ],
            "total_number_of_events": 200401
        }
    }
    """
    
    # day
    day_datetime_formatted = datetime.strptime(day, "%d")
    day_only = datetime.strftime(day_datetime_formatted, "2024-12-%d")
    
    # Humans
    # number_of_events_per_type and total_number_of_events
    number_of_events_per_type = []
    total_number_of_events = 0
    
    cassandra_container_name = 'cassandra_stelios'
    keyspace = 'prod_gharchive'
    cluster = Cluster([cassandra_container_name],port=9142)
    session = cluster.connect(keyspace)
    
    prepared_query = f"" \
        f"SELECT number_of_events, event_type FROM {keyspace}.number_of_human_events_per_type_by_day "\
        f"WHERE day = '{day_only}';"
    
    
    # Query to figure out the latest day for which data is available
    rows = session.execute(prepared_query)
    
    for row in rows:
        number_of_events_per_type.\
            append({col_name: getattr(row, col_name) for col_name in row._fields})
        total_number_of_events += row.number_of_events
    
    humans_dict_to_expose = {'day': day_only, 
                      "total_number_of_events": total_number_of_events,
                      "number_of_events_per_type": number_of_events_per_type}    
    
    # Bots
    # number_of_events_per_type and total_number_of_events
    number_of_events_per_type = []
    total_number_of_events = 0
    
    prepared_query = f""\
        f"SELECT number_of_events, event_type FROM {keyspace}.number_of_bot_events_per_type_by_day "\
        f"WHERE day = '{day_only}';"
    
    
    # Query to figure out the latest day for which data is available
    rows = session.execute(prepared_query)
    
    for row in rows:
        number_of_events_per_type.\
            append({col_name: getattr(row, col_name) for col_name in row._fields})
        total_number_of_events += row.number_of_events
    
    bots_dict_to_expose = {'day': day_only, 
                      "total_number_of_events": total_number_of_events,
                      "number_of_events_per_type": number_of_events_per_type}   
    
    combined_dict_to_expose = {"humans": humans_dict_to_expose, "bots": bots_dict_to_expose}
    
    cluster.shutdown()
    
    return jsonify(combined_dict_to_expose)

# endregion

# Screen 3: Javascript frameworks
# region

# Expose number of stars per repo by month for all repos
@app.route('/javascript_frameworks/number_of_stars_by_repo', methods=['GET'])
def get_number_of_stars_of_js_repo_by_day():
    """
    Example JSON to be exposed:
    "preactjs/preact": {
        "stars_by_day": {
            "2024-12-01": 0,
            ...
            "2024-12-30": 0
        },
        "total_stars_for_all_days": 48
    },
    "angular/angular": {
        "stars_by_day": {
            "2024-12-01": 0,
            ...
            "2024-12-30": 0
        },
        "total_stars_for_all_days": 25
    }
    """
    
    
    cassandra_container_name = 'cassandra_stelios'
    keyspace = 'prod_gharchive'
    cluster = Cluster([cassandra_container_name],port=9142)
    session = cluster.connect(keyspace)
    
    # List of Javascript repos monitored
    js_repos_list = ['marko-js/marko', 'mithriljs/mithril.js', 'angular/angular', 
    'angular/angular.js', 'emberjs/ember.js', 'knockout/knockout', 'tastejs/todomvc',
    'spine/spine', 'vuejs/vue', 'vuejs/core', 'Polymer/polymer', 'facebook/react', 
    'finom/seemple', 'aurelia/framework', 'optimizely/nuclear-js', 'jashkenas/backbone', 
    'dojo/dojo', 'jorgebucaran/hyperapp', 'riot/riot', 'daemonite/material', 
    'polymer/lit-element', 'aurelia/aurelia', 'sveltejs/svelte', 'neomjs/neo', 
    'preactjs/preact', 'hotwired/stimulus', 'alpinejs/alpine', 'solidjs/solid', 
    'ionic-team/stencil', 'jquery/jquery']
    
    # Get month and stars for every repo
    # for js_repo in js_repos_list:
    dict_to_be_exposed = {}
    
    for js_repo in js_repos_list:
        # Initialize the stars history of the js repo with zeros (we set as date arbitrarily
        # the first of the month (day = 1))
        starting_datetime = datetime(year=2024, month=12, day=1)
        stars_by_day_dict = {}
        
        for day_delta in range(31):
            new_day = starting_datetime + relativedelta(days=+day_delta)
            new_day_stringified = datetime.strftime(new_day, "%Y-%m-%d") 
            stars_by_day_dict[new_day_stringified] = 0
        
        
        
        total_stars_for_all_days = 0
        # Prepare the query
        prepared_query = f"SELECT day, number_of_stars "\
            f"FROM {keyspace}.stars_per_day_on_js_repo "\
            f"WHERE repo_name = '{js_repo}' ALLOW FILTERING;"    
        rows = session.execute(prepared_query)
        rows_list = rows.all()
        
        # Populate the dict to expose
        for row in rows_list:
            day_of_row = getattr(row, 'day')
            number_of_stars_of_day = getattr(row, 'number_of_stars')
            stars_by_day_dict[day_of_row] = number_of_stars_of_day
            total_stars_for_all_days += number_of_stars_of_day
        
        # print(dict_to_be_exposed[js_repo])
        # stars_by_day_dict['total_stars_for_all_days'] = \
        #     total_stars_for_all_days
    
        dict_to_be_exposed[js_repo] = {}
        dict_to_be_exposed[js_repo]["stars_by_day"] = stars_by_day_dict
    
        dict_to_be_exposed[js_repo]['total_stars_for_all_days'] = \
            total_stars_for_all_days
    
    cluster.shutdown()
    
    dict_to_be_exposed_ordered_by_total_number_of_stars = OrderedDict( \
        sorted(dict_to_be_exposed.items(), key=lambda x: x[1]['total_stars_for_all_days'],\
            reverse=True)
    )
    
    
    return jsonify(dict_to_be_exposed_ordered_by_total_number_of_stars)
    
    


# Expose top contributors of js repo
@app.route('/javascript_frameworks/top_contributors_of_js_repo/<path:js_repo>', methods=['GET'])
def get_top_js_repo_contributors_given_js_repo_name(js_repo):
    """
    Example JSON to be exposed:
    {
    "top_contributors_of_js_repo": [
        {
        "number_of_contributions": 206,
        "username": "mofeiZ"
        },
        ...,
        {
        "number_of_contributions": 1,
        "username": "hoxyq"
        }
    ]
    }
    """
    unquoted_js_repo = unquote(js_repo)
    cassandra_container_name = 'cassandra_stelios'
    keyspace = 'prod_gharchive'
    cluster = Cluster([cassandra_container_name],port=9142)
    session = cluster.connect(keyspace)
    
    # Top contributors of given js_repo
    # js_repo_unquoted = unquote(js_repo)
    prepared_query = f" "\
        f"SELECT * FROM {keyspace}.top_contributors_of_js_repo "\
        f"WHERE repo_name = '{unquoted_js_repo}' ALLOW FILTERING;"\
    
    # Query to figure out the latest day for which data is available
    rows = session.execute(prepared_query)
    rows_list = rows.all()
    
    username_dict = {}
    
    # Initialize the dict with zeros
    for row_list_index in range(len(rows_list)):
        # username_of_row = rows_list[row_list_index]["username"]
        username_of_row = getattr(rows_list[row_list_index], "username")
        username_dict[username_of_row] = 0
    
    # # Example output username_dict 
    # {
    #     "top_contributors_of_js_repo": [
    #         {
    #         "number_of_contributions": 0,
    #         "username": "tobiu"
    #         },
    #         {
    #         "number_of_contributions": 0,
    #         "username": "Mark_Robin"
    #         },
    #         {
    #         "number_of_contributions": 0,
    #         "username": "tobiu"
    #         },
    #         {
    #         "number_of_contributions": 0,
    #         "username": "tobiu"
    #         }
    #     ]
    # }
    
    username_list = []
    for row_list_index in range(len(rows_list)):
        # number_of_contributions_of_row = \
        #     rows_list[row_list_index]["number_of_contributions"]
        number_of_contributions_of_row = \
            getattr(rows_list[row_list_index], "number_of_contributions")
        username_of_row = getattr(rows_list[row_list_index], "username")
        username_dict[username_of_row] += number_of_contributions_of_row
    # Example output username_dict 
    # {
    #     "top_contributors_of_js_repo": [
    #         {
    #         "number_of_contributions": 2,
    #         "username": "tobiu"
    #         },
    #         {
    #         "number_of_contributions": 6,
    #         "username": "Mark_Robin"
    #         }
    #         {
    #         "number_of_contributions": 1,
    #         "username": "tobiu"
    #         },
    #         {
    #         "number_of_contributions": 2,
    #         "username": "tobiu"
    #         }
    #     ]
    # }

    for username_in_dict, number_of_contributions_in_dict in username_dict.items():
        username_list.append({"username": username_in_dict, 
                    "number_of_contributions": number_of_contributions_in_dict})
    # Example output username_list 
    # {
    #     "top_contributors_of_js_repo": [
    #         {
    #         "number_of_contributions": 5,
    #         "username": "tobiu"
    #         },
    #         {
    #         "number_of_contributions": 6,
    #         "username": "Mark_Robin"
    #         }
    #     ]
    # }                   
    
    
    val_based_sorted_list_of_dicts = sorted(username_list, 
                                key= lambda item: item["number_of_contributions"],
                                reverse=True)
    # Example output val_based_sorted_list_of_dicts 
    # {
    #     "top_contributors_of_js_repo": [
    #         {
    #         "number_of_contributions": 6,
    #         "username": "Mark_Robin"
    #         },
    #         {
    #         "number_of_contributions": 5,
    #         "username": "tobiu"
    #         }
    #     ]
    #} 
        
    return {"top_contributors_of_js_repo": val_based_sorted_list_of_dicts}
    # return jsonify({val_based_sorted_list_of_dicts})

# endregion

# Screen 4: Deep insights, issues
# region


# Common functions for apex charts bar chart info endpoints 
# '/deep_insights_issues/pull_request_closing_times_bar_chart/<path:repo_name_1>/vs/<path:repo_name_2>'
# and
# '/deep_insights_issues/issues_closing_times_bar_chart/<path:repo_name_1>/vs/<path:repo_name_2>'
def create_histogram_keyspace(cassandra_host=str, cassandra_port=int, histogram_keyspace=str):
    """
    Create histogram keyspace if not exists in the database
    """
    cluster = Cluster([cassandra_host],port=cassandra_port)
    session = cluster.connect()
    create_histogram_keyspace_query = f"CREATE KEYSPACE IF NOT EXISTS {histogram_keyspace} "\
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}" \
        "AND durable_writes = true;"
    session.execute(create_histogram_keyspace_query)
    
def create_histograms_table(cassandra_host=str, cassandra_port=int, histogram_keyspace=str, histograms_table_name=str):
    """
    Create histogram table if not exists in the database
    """
    cluster = Cluster([cassandra_host],port=cassandra_port)
    session = cluster.connect()
    create_histograms_info_table_query = f"CREATE TABLE IF NOT EXISTS {histogram_keyspace}.{histograms_table_name} "\
        "(histogram_name text, bin_centers list<double>, bin_edges list<double>, "\
            "abs_frequencies list<double>, PRIMARY KEY (histogram_name));"
    session.execute(create_histograms_info_table_query)

def calculate_histogram_info(histogram_name=str, bin_edges=list, closing_times_list=list):
            """
            Calculates histogram info (bin centers and absolute_values) given the bin_edges and the closing_times list.
            The bin edges must be in seconds.
            """
            
            # Calculate absolute frequencies
            closing_times_list_for_histogram = np.array(closing_times_list)
            abs_frequencies, _ = np.histogram(closing_times_list_for_histogram, 
                                                    bins=bin_edges)
            abs_frequencies = abs_frequencies.tolist()
            
            # Calculate bin centers
            bin_centers = []
            for bin_edge_index in range(len(bin_edges)-1):
                bin_centers.append((bin_edges[bin_edge_index]+bin_edges[bin_edge_index+1])/2)    
            print(f"Completed calculation of bin centers, bin edges and absolute values of histogram '{histogram_name}'\n"\
                f"Bin centers: {bin_centers},\n"\
                f"Bin edges: {bin_edges})\n"\
                f"Absolute frequencies: {abs_frequencies}")
            
            return bin_centers, abs_frequencies
    
def store_histogram_info_in_cassandra(cassandra_host, cassandra_port, histograms_keyspace, histograms_table_name, histogram_name, bin_centers, bin_edges, abs_frequencies):
            
            cluster = Cluster([cassandra_host],port=cassandra_port)
            session = cluster.connect()    
            # Insert bin centers, bin edges and absolute frequencies in cassandra
            insert_histogram_info = f"INSERT INTO {histograms_keyspace}.{histograms_table_name} "\
                f"(histogram_name, bin_centers, bin_edges, abs_frequencies) VALUES ('{histogram_name}', {bin_centers}, {bin_edges}, "\
                    f"{abs_frequencies});"
            session.execute(insert_histogram_info) 

def seconds_to_period(num_of_seconds):
        """
        :param num_of_seconds: number of seconds to convert to higher time durations (years, months, etc)
        :return time_dict_keep_largest_two_non_zero_durations: The first two largest time durations that the seconds are converted into:
        
        Example:
        For input num_of_seconds = 168039959, we get 
        output: time_dict_keep_largest_two_non_zero_durations = {'years': 5, 'months': 3}
        """
        seconds_in_a_year = 365*24*60*60
        seconds_in_a_month = 30*24*60*60
        seconds_in_a_day = 24*60*60
        seconds_in_an_hour = 60*60
        seconds_in_a_minute = 60

        years, remaining_seconds = divmod(num_of_seconds, seconds_in_a_year)
        months, remaining_seconds = divmod(remaining_seconds, seconds_in_a_month)
        days, remaining_seconds = divmod(remaining_seconds, seconds_in_a_day)
        hours, remaining_seconds = divmod(remaining_seconds, seconds_in_an_hour)
        minutes, remaining_seconds = divmod(remaining_seconds, seconds_in_a_minute)
        seconds = remaining_seconds

        time_dict = {'year(s)': years, 'month(s)': months, 'day(s)':days, \
            'hour(s)':hours, 'minute(s)':minutes, 'second(s)':seconds}

        time_dict_keep_largest_two_non_zero_durations = {}

        # Iterate through the ordered time dictionary
        for k, v, in time_dict.items():
            if time_dict[k] != 0:
                time_dict_keep_largest_two_non_zero_durations[k] = int(v)
            # Keep only 2 of the values in the time period
            if len(time_dict_keep_largest_two_non_zero_durations.items()) == 2:
                break
        
        if time_dict_keep_largest_two_non_zero_durations == {}:
            return {'second(s)': 0}    
        
        return time_dict_keep_largest_two_non_zero_durations

def period_to_string(time_dict_keep_largest_two_non_zero_durations):
        time_periods_to_abbrev_dict = {'year(s)': 'y', 'month(s)': 'm', 'day(s)': 'd', \
                'hour(s)': 'h', 'minute(s)': 'min', 'second(s)': 'sec'}
        time_dict_formatted = {time_periods_to_abbrev_dict[k]: v for k, v in time_dict_keep_largest_two_non_zero_durations.items()}
        time_dict_formatted_list = ['{} {}'.format(v, k) for k, v in time_dict_formatted.items()]
        time_dict_formatted_list_stringified = ', '.join(time_dict_formatted_list[0:])
        return time_dict_formatted_list_stringified

    
    # TODO: Uncomment and change the functions
    


# (For apex charts) Expose pull requests closing times for all repos and the specific selected two repos
@app.route('/deep_insights_issues/pull_request_closing_times_bar_chart/<path:repo_name_1>/vs/<path:repo_name_2>',
           methods=['GET'])
def compare_pull_request_closing_times_bar_chart(repo_name_1, repo_name_2):
    """
    Example JSON to be exposed:
    
    """
    print("Get pull-request closing times from database:\n"
        f"Repo 1: {repo_name_1}\n"
        f"Repo 2: {repo_name_2}\n")
    

    # Create histograms keyspace and table 
    cassandra_host = 'cassandra_stelios'
    cassandra_port = 9142
    histograms_keyspace = 'histograms'
    create_histogram_keyspace(cassandra_host, cassandra_port, histograms_keyspace)
    histograms_table_name = 'histograms_info'
    create_histograms_table(cassandra_host, cassandra_port, histograms_keyspace, histograms_table_name)    
    histogram_name = 'pull_requests_closing_times_histogram_bar_chart'
    
    # Query histogram info (bin centers, edges and absolute frequencies) for the given histogram name if exists
    get_pull_requests_histogram_info = f"SELECT bin_centers, bin_edges, abs_frequencies "\
        f"FROM {histograms_keyspace}.{histograms_table_name} WHERE histogram_name = '{histogram_name}';"        
    cluster = Cluster([cassandra_host],port=cassandra_port)
    session = cluster.connect() 
    row = session.execute(get_pull_requests_histogram_info)
    row_in_response = row.one()
    
    # Calculate histogram info (bin centers, edges and absolute frequencies) if its name was not found in the database (or if you want to recalculate it the process)
    if row_in_response == None:
        
        # Query all repos closing times
        keyspace = "prod_gharchive"
        print(f"Bin centers, edges and absolute values of histogram '{histogram_name}' are not in table '{histograms_keyspace}.{histograms_table_name}'.\n"\
            f"Calculating based on table {keyspace}.pull_requests_closing_times...")
        
        
        def query_pull_requests_closing_times(cassandra_host, cassandra_port):
            """
            Queries all pull requests closing times of all repos in table 'prod_gharchive.pull_requests_closing_times' and returns them as a list
            """
            keyspace = "prod_gharchive"
            prepared_query = f"SELECT repo_name, pull_request_number, opening_time, closing_time "\
                f" FROM {keyspace}.pull_request_closing_times;"    
            
            cluster = Cluster([cassandra_host],port=cassandra_port)
            session = cluster.connect()    
            rows = session.execute(prepared_query)
            rows_list = rows.all()
            # Keep only closed (with non None closing times) pull requests
            rows_list = [row for row in rows_list if getattr(row, 'closing_time') != None] 
            closing_times_list = []
            for row in rows_list:
                opening_time_of_row = getattr(row, 'opening_time')
                closing_time_of_row = getattr(row, 'closing_time')
                # Remove rows containing closing time values earlier than opening time values
                try:
                    opening_datetime = datetime.strptime(opening_time_of_row, '%Y-%m-%dT%H:%M:%SZ')
                    closing_datetime = datetime.strptime(closing_time_of_row, '%Y-%m-%dT%H:%M:%SZ')
                    time_diff = closing_datetime - opening_datetime
                    closing_time_in_seconds = time_diff.total_seconds()
                    if closing_time_in_seconds < 0:
                        print(f"Repo name: {getattr(row, 'repo_name')}\n"
                            f"Pull-request number: {getattr(row, 'pull_request_number')}\n"
                            f'Closing time: {closing_datetime} is earlier than {opening_datetime}')
                except Exception as e:
                    print(f"Exception: {e}"
                        f"Repo name: {getattr(row, 'repo_name')}\n"
                        f"Pull-request number: {getattr(row, 'pull_request_number')}\n"
                        f"Opening time of row: {opening_time_of_row}\n"
                        f"Closing time of row: {closing_time_of_row}\n")
                            
                closing_times_list.append(closing_time_in_seconds)
            
            return closing_times_list
  
        closing_times_list = query_pull_requests_closing_times(cassandra_host, cassandra_port)
    
    
        # Select the bin edges
        seconds_in_min = 60
        seconds_in_hour = 60* seconds_in_min
        seconds_in_day = 24* seconds_in_hour
        seconds_in_month = 30* seconds_in_day
        seconds_in_year = 365* seconds_in_day
        bin_edges = [0, seconds_in_min, seconds_in_hour, seconds_in_day, seconds_in_month, seconds_in_year, 10*seconds_in_year]
        # Far right bin should be the max between: max(bin_edges) and max(closing_times)
        if max(bin_edges) < max(closing_times_list):
            bin_edges.append(max(closing_times_list))

    
        bin_centers, abs_frequencies = calculate_histogram_info(histogram_name, bin_edges, closing_times_list)            

        store_histogram_info_in_cassandra(cassandra_host, cassandra_port, histograms_keyspace, histograms_table_name, histogram_name, bin_centers, bin_edges, abs_frequencies) 
        
    elif row_in_response != None:
        bin_centers = getattr(row_in_response, 'bin_centers')
        bin_edges = getattr(row_in_response, 'bin_edges')
        abs_frequencies = getattr(row_in_response, 'abs_frequencies')
        
        print(f"Bin centers, bin edges and absolute frequencies of histogram '{histogram_name}' already exist in table {histograms_keyspace}.{histograms_table_name}\n")
            
    selected_bin_edges_stringified = [period_to_string(seconds_to_period(bin_edges[i])) for i in range(len(bin_edges))]                                
    corresponding_bin_centers_labels = ['{} - {}'.format(selected_bin_edges_stringified[i], selected_bin_edges_stringified[i+1]) for i in range(len(abs_frequencies))]
        
    def get_pull_requests_closing_times_of_repo(repo_name=str): 
        keyspace = 'prod_gharchive'
        prepared_query = f"SELECT "\
            "opening_time, closing_time "\
            f"FROM {keyspace}.pull_request_closing_times WHERE repo_name = '{repo_name}';"    
        
        session = cluster.connect(keyspace)
        rows = session.execute(prepared_query)
        rows_list = rows.all()
        # Keep only closed pull requests
        rows_list = [row for row in rows_list if getattr(row, 'closing_time') != None] 
        
        
        pull_request_closing_times = []
        for row in rows_list:
            opening_time_of_row = getattr(row, 'opening_time')
            closing_time_of_row = getattr(row, 'closing_time')
            try:
                opening_datetime = datetime.strptime(opening_time_of_row, '%Y-%m-%dT%H:%M:%SZ')
                closing_datetime = datetime.strptime(closing_time_of_row, '%Y-%m-%dT%H:%M:%SZ')
                time_diff = closing_datetime - opening_datetime
                closing_time_in_seconds = time_diff.total_seconds()
                # Remove rows containing closing time values earlier than opening time values
                if closing_time_in_seconds < 0:
                    print(f"Repo name: {getattr(row, 'repo_name')}\n"
                        f"Pull-request number: {getattr(row, 'pull_request_number')}\n"
                        f"Closing time: {closing_datetime} is earlier than {opening_datetime}")    
            except Exception as e:
                print(f"Exception: {e}\n"
                    f"Repo name: {getattr(row, 'repo_name')}\n"
                    f"Pull-request number: {getattr(row, 'pull_request_number')}\n"
                    f"Opening time of row: {opening_time_of_row}\n"
                    f"Closing time of row: {closing_time_of_row}\n")
            
            pull_request_closing_times.append(closing_time_in_seconds)
        return pull_request_closing_times
        
    def get_bin_label_of_average_pull_request_closing_time_of_repo(average_pull_request_closing_time_of_repo, labels_of_bin_centers, bin_edges):
        """
        Given the bin centers and the edges of all pull request closing times in the database, find the bin center where the average of the issue closing times of a repo lies
        """
        # Create the dataset for the repo 1 containing only 1 non zero value
        for bin_edge_index in range(len(bin_edges)-1):
            if average_pull_request_closing_time_of_repo >= bin_edges[bin_edge_index] and \
                average_pull_request_closing_time_of_repo < bin_edges[bin_edge_index+1]:
                bin_label_of_average_pull_request_closing_time_of_repo = \
                    labels_of_bin_centers[bin_edge_index]
                # Once the interval in which the average closing time lies, break the loop
                break
        return bin_label_of_average_pull_request_closing_time_of_repo
    
    pull_request_closing_times_of_repo_1 = get_pull_requests_closing_times_of_repo(repo_name_1)
    average_closing_time_of_repo_1 = np.average(pull_request_closing_times_of_repo_1)
    average_closing_time_of_repo_1_stringified = period_to_string(seconds_to_period(average_closing_time_of_repo_1))
    bin_label_of_average_pull_request_closing_time_of_repo_1 = \
        get_bin_label_of_average_pull_request_closing_time_of_repo(average_closing_time_of_repo_1, corresponding_bin_centers_labels, bin_edges)
    
    
    pull_request_closing_times_of_repo_2 = get_pull_requests_closing_times_of_repo(repo_name_2)
    average_closing_time_of_repo_2 = np.average(pull_request_closing_times_of_repo_2)
    average_closing_time_of_repo_2_stringified = period_to_string(seconds_to_period(average_closing_time_of_repo_2))
    bin_label_of_average_pull_request_closing_time_of_repo_2 = \
        get_bin_label_of_average_pull_request_closing_time_of_repo(average_closing_time_of_repo_2, corresponding_bin_centers_labels, bin_edges)
    
    
    dict_to_be_exposed = {}
    dict_to_be_exposed["selected_bin_edges_in_seconds"] = bin_edges
    dict_to_be_exposed["selected_bin_edges_stringified"] = selected_bin_edges_stringified
    dict_to_be_exposed["corresponding_bin_centers_labels"] = corresponding_bin_centers_labels
    dict_to_be_exposed["abs_frequencies"] = abs_frequencies
        
    dict_to_be_exposed["repo_name_1"] = repo_name_1
    dict_to_be_exposed["average_pull_request_closing_time_of_repo_1_in_seconds"] = average_closing_time_of_repo_1 # e.g: 3646
    dict_to_be_exposed["average_pull_request_closing_time_of_repo_1_stringified"] = average_closing_time_of_repo_1_stringified # e.g: 1 h, 46 sec
    dict_to_be_exposed["bin_center_label_of_average_pull_request_closing_time_of_repo_1"] = bin_label_of_average_pull_request_closing_time_of_repo_1 # e.g '1 h - 1 d'
    
    dict_to_be_exposed["repo_name_2"] = repo_name_2
    dict_to_be_exposed["average_pull_request_closing_time_of_repo_2_in_seconds"] = average_closing_time_of_repo_2
    dict_to_be_exposed["average_pull_request_closing_time_of_repo_2_stringified"] = average_closing_time_of_repo_2_stringified
    dict_to_be_exposed["bin_center_label_of_average_pull_request_closing_time_of_repo_2"] = bin_label_of_average_pull_request_closing_time_of_repo_2
    
    
    cluster.shutdown()
    
    return jsonify(dict_to_be_exposed)


# (For chart.js) Expose pull requests closing times for all repos and the specific selected two repos
@app.route('/deep_insights_issues/pull_request_closing_times/<path:repo_name_1>/vs/<path:repo_name_2>',
           methods=['GET'])
def compare_pull_request_closing_times(repo_name_1, repo_name_2):
    """
    Example JSON to be exposed:
    
    """
    print("Get pull-request closing times from database:\n"
        f"Repo 1: {repo_name_1}\n"
        f"Repo 2: {repo_name_2}\n")
    
    cassandra_container_name = 'cassandra_stelios'
    cluster = Cluster([cassandra_container_name],port=9142)
    session = cluster.connect()
    histogram_keyspace = 'histograms'
    create_histogram_query = f"CREATE KEYSPACE IF NOT EXISTS {histogram_keyspace} "\
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}" \
        "AND durable_writes = true;"
    session.execute(create_histogram_query)
    
    histograms_table_name = 'histograms_info'
    create_histograms_info_table_query = f"CREATE TABLE IF NOT EXISTS {histogram_keyspace}.{histograms_table_name} "\
        "(histogram_name text, bin_centers list<double>, bin_edges list<double>, "\
            "abs_frequencies list<double>, PRIMARY KEY (histogram_name));"
    session.execute(create_histograms_info_table_query)
    
    histogram_name = 'pull_requests_closing_times_histogram'
    get_pull_requests_histogram_info = f"SELECT bin_centers, bin_edges, abs_frequencies "\
        f"FROM {histogram_keyspace}.{histograms_table_name} WHERE histogram_name = '{histogram_name}';"        
    row = session.execute(get_pull_requests_histogram_info)
    row_in_response = row.one()
    
    
    if row_in_response == None:
        
        keyspace = "prod_gharchive"
        # Calculate the histogram values
        print(f"Bin centers, edges and absolute values of histogram '{histogram_name}' are not in table '{histogram_keyspace}.{histograms_table_name}'.\n"\
            f"Calculating based on table {keyspace}.pull_requests_closing_times...")
        
        prepared_query = f"SELECT repo_name, pull_request_number, opening_time, closing_time "\
        f" FROM {keyspace}.pull_request_closing_times;"    
        
        rows = session.execute(prepared_query)
        rows_list = rows.all()
        # Keep only closed (with non None closing times) pull requests
        rows_list = [row for row in rows_list if getattr(row, 'closing_time') != None] 
        
        # Get all repos' closing times 
        closing_times_list = []
        for row in rows_list:
            opening_time_of_row = getattr(row, 'opening_time')
            closing_time_of_row = getattr(row, 'closing_time')
            # Remove rows containing closing time values earlier than opening time values
            try:
                opening_datetime = datetime.strptime(opening_time_of_row, '%Y-%m-%dT%H:%M:%SZ')
                closing_datetime = datetime.strptime(closing_time_of_row, '%Y-%m-%dT%H:%M:%SZ')
                time_diff = closing_datetime - opening_datetime
                closing_time_in_seconds = time_diff.total_seconds()
                if closing_time_in_seconds < 0:
                    print(f"Repo name: {getattr(row, 'repo_name')}\n"
                        f"Pull-request number: {getattr(row, 'pull_request_number')}\n"
                        f'Closing time: {closing_datetime} is earlier than {opening_datetime}')
            except Exception as e:
                print(f"Exception: {e}"
                    f"Repo name: {getattr(row, 'repo_name')}\n"
                    f"Pull-request number: {getattr(row, 'pull_request_number')}\n"
                    f"Opening time of row: {opening_time_of_row}\n"
                    f"Closing time of row: {closing_time_of_row}\n")
                        
            closing_times_list.append(closing_time_in_seconds)
    
    
        # Create the histogram of the closing times values 
        # Transform left-skewed distribution to visualise it
        closing_times_list_no_skew = np.log10(np.array(closing_times_list) + 1)
        
        # Get the bin edges
        num_of_bins_for_the_histogram = 10
        abs_frequencies, bin_edges = np.histogram(closing_times_list_no_skew, 
                                                bins=num_of_bins_for_the_histogram)
        
        abs_frequencies = abs_frequencies.tolist()
        bin_edges = bin_edges.tolist()
         
        # Calculate the bin centers from the bin edges
        bin_centers = []
        # length(bin_centers) = length(bin_edges) - 1
        for bin_edge_index in range(len(bin_edges)-1):
            bin_centers.append((bin_edges[bin_edge_index]+bin_edges[bin_edge_index+1])/2)    
        print(f"Completed calculation of bin centers, bin edges and absolute values of histogram '{histogram_name}'\n"\
            f"Bin centers: {bin_centers},\n"\
            f"Bin edges: {bin_edges})\n"\
            f"Absolute frequencies: {abs_frequencies}")
        
        
        insert_histogram_info = f"INSERT INTO {histogram_keyspace}.{histograms_table_name} "\
            f"(histogram_name, bin_centers, bin_edges, abs_frequencies) VALUES ('{histogram_name}', {bin_centers}, {bin_edges}, "\
                f"{abs_frequencies});"
   
        session.execute(insert_histogram_info)
        
    elif row_in_response != None:
        bin_centers = getattr(row_in_response, 'bin_centers')
        bin_edges = getattr(row_in_response, 'bin_edges')
        abs_frequencies = getattr(row_in_response, 'abs_frequencies')
        
        print(f"Bin centers, bin edges and absolute frequencies of histogram '{histogram_name}' already exist in table {histogram_keyspace}.{histograms_table_name}\n")
            
    
    def get_pull_requests_closing_times_of_repo(repo_name=str): 
        keyspace = 'prod_gharchive'
        prepared_query = f"SELECT "\
            "opening_time, closing_time "\
            f"FROM {keyspace}.pull_request_closing_times WHERE repo_name = '{repo_name}';"    
        
        session = cluster.connect(keyspace)
        rows = session.execute(prepared_query)
        rows_list = rows.all()
        # Keep only closed pull requests
        rows_list = [row for row in rows_list if getattr(row, 'closing_time') != None] 
        
        
        pull_request_closing_times = []
        for row in rows_list:
            opening_time_of_row = getattr(row, 'opening_time')
            closing_time_of_row = getattr(row, 'closing_time')
            try:
                opening_datetime = datetime.strptime(opening_time_of_row, '%Y-%m-%dT%H:%M:%SZ')
                closing_datetime = datetime.strptime(closing_time_of_row, '%Y-%m-%dT%H:%M:%SZ')
                time_diff = closing_datetime - opening_datetime
                closing_time_in_seconds = time_diff.total_seconds()
                # Remove rows containing closing time values earlier than opening time values
                if closing_time_in_seconds < 0:
                    print(f"Repo name: {getattr(row, 'repo_name')}\n"
                        f"Pull-request number: {getattr(row, 'pull_request_number')}\n"
                        f"Closing time: {closing_datetime} is earlier than {opening_datetime}")    
            except Exception as e:
                print(f"Exception: {e}\n"
                    f"Repo name: {getattr(row, 'repo_name')}\n"
                    f"Pull-request number: {getattr(row, 'pull_request_number')}\n"
                    f"Opening time of row: {opening_time_of_row}\n"
                    f"Closing time of row: {closing_time_of_row}\n")
            
            pull_request_closing_times.append(closing_time_in_seconds)
        return pull_request_closing_times
    
    def get_bin_of_average_pull_request_closing_time_of_repo(pull_request_closing_times_of_repo, bin_centers, bin_edges, abs_frequencies):
        """
        Given the bin centers and the edges of all pull request closing times in the database, find the bin center where the average of the issue closing times of a repo lies
        """
        # Get and transform the average closing time of the repo
        average_closing_time_of_repo_1_no_skew = np.log10(np.average(pull_request_closing_times_of_repo) + 1)
        
        # (0, 0, max_abs_frequency value, 0, ..., 0, 0) to make it so only bin that appears 
        # is the one with the average pull-request closing time overlapping the existing distribution
        bin_of_average_pull_request_closing_time_of_repo_1 = np.zeros((len(bin_centers)))
        # # Create the dataset for the repo 1 containing only 1 non zero value
        for bin_edge_index in range(len(bin_edges)-1):
            if average_closing_time_of_repo_1_no_skew >= bin_edges[bin_edge_index] and \
                average_closing_time_of_repo_1_no_skew < bin_edges[bin_edge_index+1]:
                bin_of_average_pull_request_closing_time_of_repo_1[bin_edge_index] = \
                    np.max(abs_frequencies)
                # Once the interval in which the average closing time lies, break the loop
                break
        return bin_of_average_pull_request_closing_time_of_repo_1
    
    pull_request_closing_times_of_repo_1 = get_pull_requests_closing_times_of_repo(repo_name_1)
    bin_of_average_pull_request_closing_time_of_repo_1 = \
        get_bin_of_average_pull_request_closing_time_of_repo(pull_request_closing_times_of_repo_1, bin_centers, bin_edges, abs_frequencies)
    
    
    pull_request_closing_times_of_repo_2 = get_pull_requests_closing_times_of_repo(repo_name_2)
    bin_of_average_pull_request_closing_time_of_repo_2 = \
        get_bin_of_average_pull_request_closing_time_of_repo(pull_request_closing_times_of_repo_2, bin_centers, bin_edges, abs_frequencies)
    
    
    dict_to_be_exposed = {}
    dict_to_be_exposed["bin_centers_log_transformed"] = bin_centers
    dict_to_be_exposed["bin_centers_in_seconds"] = (np.power(10, bin_centers) - 1).tolist()
    dict_to_be_exposed["bin_edges_log_transformed"] = bin_edges
    dict_to_be_exposed["abs_frequencies"] = abs_frequencies
    # # Uncomment to expose all closing times
    # dict_to_be_exposed["closing_times"] = closing_times_list_no_skew
    dict_to_be_exposed["repo_name_1"] = repo_name_1
    dict_to_be_exposed["bin_of_average_pull_request_closing_time_of_repo_1"] = \
        bin_of_average_pull_request_closing_time_of_repo_1.tolist()
    dict_to_be_exposed["repo_name_2"] = repo_name_2
    dict_to_be_exposed["bin_of_average_pull_request_closing_time_of_repo_2"] = \
        bin_of_average_pull_request_closing_time_of_repo_2.tolist()
    
    cluster.shutdown()
    
    return jsonify(dict_to_be_exposed)



# (For apex charts) Expose issues closing times for all repos and the specific selected two repos
@app.route('/deep_insights_issues/issues_closing_times_bar_chart/<path:repo_name_1>/vs/<path:repo_name_2>',
           methods=['GET'])
def compare_issue_closing_times_bar_chart(repo_name_1, repo_name_2):
    """
    Example JSON to be exposed:
    
    """
    print("Get issues closing times from database:\n"
        f"Repo 1: {repo_name_1}\n"
        f"Repo 2: {repo_name_2}\n")
    
    
    def create_histogram_keyspace(cassandra_host=str, cassandra_port=int, histogram_keyspace=str):
        """
        Create histogram keyspace if not exists in the database
        """
        cluster = Cluster([cassandra_host],port=cassandra_port)
        session = cluster.connect()
        create_histogram_keyspace_query = f"CREATE KEYSPACE IF NOT EXISTS {histogram_keyspace} "\
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}" \
            "AND durable_writes = true;"
        session.execute(create_histogram_keyspace_query)
        
    def create_histograms_table(cassandra_host=str, cassandra_port=int, histogram_keyspace=str, histograms_table_name=str):
        """
        Create histogram table if not exists in the database
        """
        cluster = Cluster([cassandra_host],port=cassandra_port)
        session = cluster.connect()
        create_histograms_info_table_query = f"CREATE TABLE IF NOT EXISTS {histogram_keyspace}.{histograms_table_name} "\
            "(histogram_name text, bin_centers list<double>, bin_edges list<double>, "\
                "abs_frequencies list<double>, PRIMARY KEY (histogram_name));"
        session.execute(create_histograms_info_table_query)
        


    # Create histograms keyspace and table 
    cassandra_host = 'cassandra_stelios'
    cassandra_port = 9142
    histograms_keyspace = 'histograms'
    create_histogram_keyspace(cassandra_host, cassandra_port, histograms_keyspace)
    histograms_table_name = 'histograms_info'
    create_histograms_table(cassandra_host, cassandra_port, histograms_keyspace, histograms_table_name)


    
    
    histogram_name = 'issues_closing_times_histogram_bar_chart'
    
    # Query histogram info (bin centers, edges and absolute frequencies) for the given histogram name if exists
    get_issues_histogram_info = f"SELECT bin_centers, bin_edges, abs_frequencies "\
        f"FROM {histograms_keyspace}.{histograms_table_name} WHERE histogram_name = '{histogram_name}';"        
    cluster = Cluster([cassandra_host],port=cassandra_port)
    session = cluster.connect() 
    row = session.execute(get_issues_histogram_info)
    row_in_response = row.one()
    
    # Calculate histogram info (bin centers, edges and absolute frequencies) if its name was not found in the database (or if you want to recalculate it the process)
    if row_in_response == None:
        
        # Query all repos closing times
        keyspace = "prod_gharchive"
        print(f"Bin centers, edges and absolute values of histogram '{histogram_name}' are not in table '{histograms_keyspace}.{histograms_table_name}'.\n"\
            f"Calculating based on table {keyspace}.issue_closing_times...")
        
        def query_issues_closing_times(cassandra_host, cassandra_port):
                """
                Queries all issues closing times of all repos in table 'prod_gharchive.issue_closing_times' and returns them as a list
                """
                keyspace = "prod_gharchive"
                prepared_query = f"SELECT repo_name, issue_number, opening_time, closing_time "\
                    f" FROM {keyspace}.issue_closing_times;"    
                
                cluster = Cluster([cassandra_host],port=cassandra_port)
                session = cluster.connect()    
                rows = session.execute(prepared_query)
                rows_list = rows.all()
                # Keep only closed (with non None closing times) issues
                rows_list = [row for row in rows_list if getattr(row, 'closing_time') != None] 
                closing_times_list = []
                for row in rows_list:
                    opening_time_of_row = getattr(row, 'opening_time')
                    closing_time_of_row = getattr(row, 'closing_time')
                    # Remove rows containing closing time values earlier than opening time values
                    try:
                        opening_datetime = datetime.strptime(opening_time_of_row, '%Y-%m-%dT%H:%M:%SZ')
                        closing_datetime = datetime.strptime(closing_time_of_row, '%Y-%m-%dT%H:%M:%SZ')
                        time_diff = closing_datetime - opening_datetime
                        closing_time_in_seconds = time_diff.total_seconds()
                        if closing_time_in_seconds < 0:
                            print(f"Repo name: {getattr(row, 'repo_name')}\n"
                                f"Issue number: {getattr(row, 'issue_number')}\n"
                                f'Closing time: {closing_datetime} is earlier than {opening_datetime}')
                    except Exception as e:
                        print(f"Exception: {e}"
                            f"Repo name: {getattr(row, 'repo_name')}\n"
                            f"Issue number: {getattr(row, 'issue_number')}\n"
                            f"Opening time of row: {opening_time_of_row}\n"
                            f"Closing time of row: {closing_time_of_row}\n")
                                
                    closing_times_list.append(closing_time_in_seconds)
                
                return closing_times_list
            
        closing_times_list = query_issues_closing_times(cassandra_host, cassandra_port)
    
    
        # Select the bin edges
        seconds_in_min = 60
        seconds_in_hour = 60* seconds_in_min
        seconds_in_day = 24* seconds_in_hour
        seconds_in_month = 30* seconds_in_day
        seconds_in_year = 365* seconds_in_day
        bin_edges = [0, seconds_in_min, seconds_in_hour, seconds_in_day, seconds_in_month, seconds_in_year, 10*seconds_in_year]
        # Far right bin should be the max between: max(bin_edges) and max(closing_times)
        if max(bin_edges) < max(closing_times_list):
            bin_edges.append(max(closing_times_list))


        # def calculate_histogram_info(bin_edges=list, closing_times_list=list):
        #     """
        #     Calculates histogram info (bin centers and absolute_values) given the bin_edges and the closing_times list.
        #     The bin edges must be in seconds.
        #     """
            
        #     # Calculate absolute frequencies
        #     closing_times_list_for_histogram = np.array(closing_times_list)
        #     abs_frequencies, _ = np.histogram(closing_times_list_for_histogram, 
        #                                             bins=bin_edges)
        #     abs_frequencies = abs_frequencies.tolist()
            
        #     # Calculate bin centers
        #     bin_centers = []
        #     for bin_edge_index in range(len(bin_edges)-1):
        #         bin_centers.append((bin_edges[bin_edge_index]+bin_edges[bin_edge_index+1])/2)    
        #     print(f"Completed calculation of bin centers, bin edges and absolute values of histogram '{histogram_name}'\n"\
        #         f"Bin centers: {bin_centers},\n"\
        #         f"Bin edges: {bin_edges})\n"\
        #         f"Absolute frequencies: {abs_frequencies}")
            
        #     return bin_centers, abs_frequencies
    
        bin_centers, abs_frequencies = calculate_histogram_info(histogram_name, bin_edges, closing_times_list)            
            
        # def store_histogram_info_in_cassandra(cassandra_host, cassandra_port, histograms_keyspace, histograms_table_name, histogram_name, bin_centers, bin_edges, abs_frequencies):
            
        #     cluster = Cluster([cassandra_host],port=cassandra_port)
        #     session = cluster.connect()    
        #     # Insert bin centers, bin edges and absolute frequencies in cassandra
        #     insert_histogram_info = f"INSERT INTO {histograms_keyspace}.{histograms_table_name} "\
        #         f"(histogram_name, bin_centers, bin_edges, abs_frequencies) VALUES ('{histogram_name}', {bin_centers}, {bin_edges}, "\
        #             f"{abs_frequencies});"
        #     session.execute(insert_histogram_info) 

        store_histogram_info_in_cassandra(cassandra_host, cassandra_port, histograms_keyspace, histograms_table_name, histogram_name, bin_centers, bin_edges, abs_frequencies) 
        
    elif row_in_response != None:
        bin_centers = getattr(row_in_response, 'bin_centers')
        bin_edges = getattr(row_in_response, 'bin_edges')
        abs_frequencies = getattr(row_in_response, 'abs_frequencies')
        
        print(f"Bin centers, bin edges and absolute frequencies of histogram '{histogram_name}' already exist in table {histograms_keyspace}.{histograms_table_name}\n")
            
    
    
    
        

    # def seconds_to_period(num_of_seconds):
    #     """
    #     :param num_of_seconds: number of seconds to convert to higher time durations (years, months, etc)
    #     :return time_dict_keep_largest_two_non_zero_durations: The first two largest time durations that the seconds are converted into:
        
    #     Example:
    #     For input num_of_seconds = 168039959, we get 
    #     output: time_dict_keep_largest_two_non_zero_durations = {'years': 5, 'months': 3}
    #     """
    #     seconds_in_a_year = 365*24*60*60
    #     seconds_in_a_month = 30*24*60*60
    #     seconds_in_a_day = 24*60*60
    #     seconds_in_an_hour = 60*60
    #     seconds_in_a_minute = 60

    #     years, remaining_seconds = divmod(num_of_seconds, seconds_in_a_year)
    #     months, remaining_seconds = divmod(remaining_seconds, seconds_in_a_month)
    #     days, remaining_seconds = divmod(remaining_seconds, seconds_in_a_day)
    #     hours, remaining_seconds = divmod(remaining_seconds, seconds_in_an_hour)
    #     minutes, remaining_seconds = divmod(remaining_seconds, seconds_in_a_minute)
    #     seconds = remaining_seconds

    #     time_dict = {'year(s)': years, 'month(s)': months, 'day(s)':days, \
    #         'hour(s)':hours, 'minute(s)':minutes, 'second(s)':seconds}

    #     time_dict_keep_largest_two_non_zero_durations = {}

    #     # Iterate through the ordered time dictionary
    #     for k, v, in time_dict.items():
    #         if time_dict[k] != 0:
    #             time_dict_keep_largest_two_non_zero_durations[k] = int(v)
    #         # Keep only 2 of the values in the time period
    #         if len(time_dict_keep_largest_two_non_zero_durations.items()) == 2:
    #             break
        
    #     if time_dict_keep_largest_two_non_zero_durations == {}:
    #         return {'second(s)': 0}    
        
    #     return time_dict_keep_largest_two_non_zero_durations

    # def period_to_string(time_dict_keep_largest_two_non_zero_durations):
    #     time_periods_to_abbrev_dict = {'year(s)': 'y', 'month(s)': 'm', 'day(s)': 'd', \
    #             'hour(s)': 'h', 'minute(s)': 'min', 'second(s)': 'sec'}
    #     time_dict_formatted = {time_periods_to_abbrev_dict[k]: v for k, v in time_dict_keep_largest_two_non_zero_durations.items()}
    #     time_dict_formatted_list = ['{} {}'.format(v, k) for k, v in time_dict_formatted.items()]
    #     time_dict_formatted_list_stringified = ', '.join(time_dict_formatted_list[0:])
    #     return time_dict_formatted_list_stringified



    selected_bin_edges_stringified = [period_to_string(seconds_to_period(bin_edges[i])) for i in range(len(bin_edges))]                                
    corresponding_bin_centers_labels = ['{} - {}'.format(selected_bin_edges_stringified[i], selected_bin_edges_stringified[i+1]) for i in range(len(abs_frequencies))]

        
        
    def get_issues_closing_times_of_repo(repo_name=str): 
        keyspace = 'prod_gharchive'
        prepared_query = f"SELECT "\
            "opening_time, closing_time "\
            f"FROM {keyspace}.issue_closing_times WHERE repo_name = '{repo_name}';"    
        
        session = cluster.connect(keyspace)
        rows = session.execute(prepared_query)
        rows_list = rows.all()
        # Keep only closed issues
        rows_list = [row for row in rows_list if getattr(row, 'closing_time') != None] 
        
        
        issues_closing_times = []
        for row in rows_list:
            opening_time_of_row = getattr(row, 'opening_time')
            closing_time_of_row = getattr(row, 'closing_time')
            try:
                opening_datetime = datetime.strptime(opening_time_of_row, '%Y-%m-%dT%H:%M:%SZ')
                closing_datetime = datetime.strptime(closing_time_of_row, '%Y-%m-%dT%H:%M:%SZ')
                time_diff = closing_datetime - opening_datetime
                closing_time_in_seconds = time_diff.total_seconds()
                # Remove rows containing closing time values earlier than opening time values
                if closing_time_in_seconds < 0:
                    print(f"Repo name: {getattr(row, 'repo_name')}\n"
                        f"Issue number: {getattr(row, 'issue_number')}\n"
                        f"Closing time: {closing_datetime} is earlier than {opening_datetime}")    
            except Exception as e:
                print(f"Exception: {e}\n"
                    f"Repo name: {getattr(row, 'repo_name')}\n"
                    f"Issue number: {getattr(row, 'issue_number')}\n"
                    f"Opening time of row: {opening_time_of_row}\n"
                    f"Closing time of row: {closing_time_of_row}\n")
            
            issues_closing_times.append(closing_time_in_seconds)
        return issues_closing_times
    
    
        
        
    def get_bin_label_of_average_issue_closing_time_of_repo(average_issue_closing_time_of_repo, labels_of_bin_centers, bin_edges):
        """
        Given the bin centers and the edges of all issues closing times in the database, find the bin center where the average of the issues closing times of a repo lies
        """
        # Create the dataset for the repo 1 containing only 1 non zero value
        for bin_edge_index in range(len(bin_edges)-1):
            if average_issue_closing_time_of_repo >= bin_edges[bin_edge_index] and \
                average_issue_closing_time_of_repo < bin_edges[bin_edge_index+1]:
                bin_label_of_average_issues_closing_time_of_repo = \
                    labels_of_bin_centers[bin_edge_index]
                # Once the interval in which the average closing time lies, break the loop
                break
        return bin_label_of_average_issues_closing_time_of_repo
    
    
    
    issue_closing_times_of_repo_1 = get_issues_closing_times_of_repo(repo_name_1)
    average_closing_time_of_repo_1 = np.average(issue_closing_times_of_repo_1)
    average_closing_time_of_repo_1_stringified = period_to_string(seconds_to_period(average_closing_time_of_repo_1))
    bin_label_of_average_issue_closing_time_of_repo_1 = \
        get_bin_label_of_average_issue_closing_time_of_repo(average_closing_time_of_repo_1, corresponding_bin_centers_labels, bin_edges)
    
    
    issue_closing_times_of_repo_2 = get_issues_closing_times_of_repo(repo_name_2)
    average_closing_time_of_repo_2 = np.average(issue_closing_times_of_repo_2)
    average_closing_time_of_repo_2_stringified = period_to_string(seconds_to_period(average_closing_time_of_repo_2))
    bin_label_of_average_issue_closing_time_of_repo_2 = \
        get_bin_label_of_average_issue_closing_time_of_repo(average_closing_time_of_repo_2, corresponding_bin_centers_labels, bin_edges)
    
    
    dict_to_be_exposed = {}
    dict_to_be_exposed["selected_bin_edges_in_seconds"] = bin_edges
    dict_to_be_exposed["selected_bin_edges_stringified"] = selected_bin_edges_stringified
    dict_to_be_exposed["corresponding_bin_centers_labels"] = corresponding_bin_centers_labels
    dict_to_be_exposed["abs_frequencies"] = abs_frequencies
        
    dict_to_be_exposed["repo_name_1"] = repo_name_1
    dict_to_be_exposed["average_issue_closing_time_of_repo_1_in_seconds"] = average_closing_time_of_repo_1 # e.g: 3646
    dict_to_be_exposed["average_issue_closing_time_of_repo_1_stringified"] = average_closing_time_of_repo_1_stringified # e.g: 1 h, 46 sec
    dict_to_be_exposed["bin_center_label_of_average_issue_closing_time_of_repo_1"] = bin_label_of_average_issue_closing_time_of_repo_1 # e.g '1 h - 1 d'
    
    dict_to_be_exposed["repo_name_2"] = repo_name_2
    dict_to_be_exposed["average_issue_closing_time_of_repo_2_in_seconds"] = average_closing_time_of_repo_2
    dict_to_be_exposed["average_issue_closing_time_of_repo_2_stringified"] = average_closing_time_of_repo_2_stringified
    dict_to_be_exposed["bin_center_label_of_average_issue_closing_time_of_repo_2"] = bin_label_of_average_issue_closing_time_of_repo_2
    
    
    cluster.shutdown()
    
    return jsonify(dict_to_be_exposed)



# (For chart.js) Expose issues closing times for all repos and the specific selected two repos
@app.route('/deep_insights_issues/issues_closing_times/<path:repo_name_1>/vs/<path:repo_name_2>',
           methods=['GET'])
def compare_issues_closing_times(repo_name_1, repo_name_2):
    """
    Example JSON to be exposed:
    
    """
    print(f"Get issues' closing times from database:\n"
        f"Repo 1: {repo_name_1}\n"
        f"Repo 2: {repo_name_2}\n")
    
    
    cassandra_container_name = 'cassandra_stelios'
    cluster = Cluster([cassandra_container_name],port=9142)
    session = cluster.connect()
    histogram_keyspace = 'histograms'
    create_histogram_query = f"CREATE KEYSPACE IF NOT EXISTS {histogram_keyspace} "\
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}" \
        "AND durable_writes = true;"
    session.execute(create_histogram_query)
    
    histograms_table_name = 'histograms_info'
    create_histograms_info_table_query = f"CREATE TABLE IF NOT EXISTS {histogram_keyspace}.{histograms_table_name} "\
        "(histogram_name text, bin_centers list<double>, bin_edges list<double>, "\
            "abs_frequencies list<double>, PRIMARY KEY (histogram_name));"
    session.execute(create_histograms_info_table_query)
    
    histogram_name = 'issues_closing_times_histogram'
    get_pull_requests_histogram_info = f"SELECT bin_centers, bin_edges, abs_frequencies "\
        f"FROM {histogram_keyspace}.{histograms_table_name} WHERE histogram_name = '{histogram_name}';"        
    row = session.execute(get_pull_requests_histogram_info)
    row_in_response = row.one()
    
    

    if row_in_response == None:
        keyspace = 'prod_gharchive'
        
        # Calculate the histogram values
        print(f"Bin centers, edges and absolute values of histogram '{histogram_name}' are not in table '{histogram_keyspace}.{histograms_table_name}'.\n"\
            f"Calculating based on table {keyspace}.issues_closing_times...")
        
        
        prepared_query = f"SELECT repo_name, issue_number, opening_time, closing_time "\
            f"FROM {keyspace}.issue_closing_times;"    
        
        rows = session.execute(prepared_query)
        rows_list = rows.all()
        # Keep only closed (with non None closing times) issues
        rows_list = [row for row in rows_list if getattr(row, 'closing_time')!= None]
        
        # Get all repos' closing times 
        closing_times_list = []
        for row in rows_list:
            opening_time_of_row = getattr(row, 'opening_time')
            closing_time_of_row = getattr(row, 'closing_time')
            # Remove rows containing closing time values earlier than opening time values
            try:
                opening_datetime = datetime.strptime(opening_time_of_row, '%Y-%m-%dT%H:%M:%SZ')
                closing_datetime = datetime.strptime(closing_time_of_row, '%Y-%m-%dT%H:%M:%SZ')
                time_diff = closing_datetime - opening_datetime
                closing_time_in_seconds = time_diff.total_seconds()
                if closing_time_in_seconds < 0:
                    print(f"Repo name: {getattr(row, 'repo_name')}\n"
                        f"Issue number: {getattr(row, 'issue_number')}\n"
                        f"Closing time: {closing_datetime} is earlier than {opening_datetime}")
                    
    
            except Exception as e:
                print(f"Exception: {e}\n"
                    f"Repo name: {getattr(row, 'repo_name')}"
                    f"Issue number: {getattr(row, 'issue_number')}\n"
                    f"Opening time of row: {opening_time_of_row}\n"
                    f"Closing time of row: {closing_time_of_row}\n")
                
            
            closing_times_list.append(closing_time_in_seconds)
    
        # Create the histogram of the closing times values 
        # Transform left-skewed distribution to visualise it
        closing_times_list_no_skew = np.log10(np.array(closing_times_list) + 1)
        
        # Get the bin edges
        num_of_bins_for_the_histogram = 10
        abs_frequencies, bin_edges = np.histogram(closing_times_list_no_skew, 
                                                bins=num_of_bins_for_the_histogram)
        
        abs_frequencies = abs_frequencies.tolist()
        bin_edges = bin_edges.tolist()
         
        # Calculate the bin centers from the bin edges
        bin_centers = []
        # length(bin_centers) = length(bin_edges) - 1
        for bin_edge_index in range(len(bin_edges)-1):
            bin_center = (bin_edges[bin_edge_index]+bin_edges[bin_edge_index+1])/2
            bin_centers.append(bin_center)    
        print(f"Completed calculation of bin centers, bin edges and absolute values of histogram '{histogram_name}'\n"\
            f"Bin centers: {bin_centers},\n"\
            f"Bin edges: {bin_edges})\n"\
            f"Absolute frequencies: {abs_frequencies}")
        
        insert_histogram_info = f"INSERT INTO {histogram_keyspace}.{histograms_table_name} "\
            f"(histogram_name, bin_centers, bin_edges, abs_frequencies) VALUES ('{histogram_name}', {bin_centers}, {bin_edges}, "\
                f"{abs_frequencies});"
   
        session.execute(insert_histogram_info)
    
    elif row_in_response != None:
        bin_centers = getattr(row_in_response, 'bin_centers')
        bin_edges = getattr(row_in_response, 'bin_edges')
        abs_frequencies = getattr(row_in_response, 'abs_frequencies')
        
        print(f"Bin centers, bin edges and absolute frequencies of histogram '{histogram_name}' already exist in table {histogram_keyspace}.{histograms_table_name}\n")
            
    def get_issues_closing_times_of_repo(repo_name=str): 
        keyspace = 'prod_gharchive'
        prepared_query = f"SELECT "\
            "opening_time, closing_time "\
            f"FROM {keyspace}.issue_closing_times WHERE repo_name = '{repo_name}';"    
        
        session = cluster.connect(keyspace)
        rows = session.execute(prepared_query)
        rows_list = rows.all()
        # Keep only closed pull requests
        rows_list = [row for row in rows_list if getattr(row, 'closing_time') != None] 
        
        
        issue_closing_times = []
        for row in rows_list:
            opening_time_of_row = getattr(row, 'opening_time')
            closing_time_of_row = getattr(row, 'closing_time')
            try:
                opening_datetime = datetime.strptime(opening_time_of_row, '%Y-%m-%dT%H:%M:%SZ')
                closing_datetime = datetime.strptime(closing_time_of_row, '%Y-%m-%dT%H:%M:%SZ')
                time_diff = closing_datetime - opening_datetime
                closing_time_in_seconds = time_diff.total_seconds()
                # Remove rows containing closing time values earlier than opening time values
                if closing_time_in_seconds < 0:
                    print(f"Repo name: {getattr(row, 'repo_name')}\n"
                        f"Issue number: {getattr(row, 'issue_number')}\n"
                        f"Closing time: {closing_datetime} is earlier than {opening_datetime}")    
            except Exception as e:
                print(f"Exception: {e}\n"
                    f"Repo name: {getattr(row, 'repo_name')}\n"
                    f"Issue number: {getattr(row, 'issue_number')}\n"
                    f"Opening time of row: {opening_time_of_row}\n"
                    f"Closing time of row: {closing_time_of_row}\n")
            
            issue_closing_times.append(closing_time_in_seconds)
        return issue_closing_times
    
    def get_bin_of_average_issue_closing_time_of_repo(issue_closing_times_of_repo, bin_centers, bin_edges, abs_frequencies):
        """
        Given the bin centers and the edges of all issues closing times in the database, find the bin center where the average of the issue closing times of a repo lies
        """
        # Get and transform the average closing time of the repo
        average_closing_time_of_repo_1_no_skew = np.log10(np.average(issue_closing_times_of_repo) + 1)
        
        # (0, 0, max_abs_frequency value, 0, ..., 0, 0) to make it so only bin that appears 
        # is the one with the average pull-request closing time overlapping the existing distribution
        bin_of_average_pull_request_closing_time_of_repo_1 = np.zeros((len(bin_centers)))
        # # Create the dataset for the repo 1 containing only 1 non zero value
        for bin_edge_index in range(len(bin_edges)-1):
            if average_closing_time_of_repo_1_no_skew >= bin_edges[bin_edge_index] and \
                average_closing_time_of_repo_1_no_skew < bin_edges[bin_edge_index+1]:
                bin_of_average_pull_request_closing_time_of_repo_1[bin_edge_index] = \
                    np.max(abs_frequencies)
                # Once the interval in which the average closing time lies, break the loop
                break
        return bin_of_average_pull_request_closing_time_of_repo_1
    
    issue_closing_times_of_repo_1 = get_issues_closing_times_of_repo(repo_name_1)
    bin_of_average_issue_closing_time_of_repo_1 = \
        get_bin_of_average_issue_closing_time_of_repo(issue_closing_times_of_repo_1, bin_centers, bin_edges, abs_frequencies)
    
    
    issue_closing_times_of_repo_2 = get_issues_closing_times_of_repo(repo_name_2)
    bin_of_average_issue_closing_time_of_repo_2 = \
        get_bin_of_average_issue_closing_time_of_repo(issue_closing_times_of_repo_2, bin_centers, bin_edges, abs_frequencies)
    
    
    
    
    # Expose data
    dict_to_be_exposed = {}
    dict_to_be_exposed["bin_centers_log_transformed"] = bin_centers
    dict_to_be_exposed["bin_centers_in_seconds"] = (np.power(10, bin_centers) - 1).tolist()
    dict_to_be_exposed["bin_edges_log_transformed"] = bin_edges
    dict_to_be_exposed["abs_frequencies"] = abs_frequencies
    # # Uncomment to expose all closing times
    # dict_to_be_exposed["closing_times_log_transformed"] = \
        # sorted(closing_times_list_no_skew.tolist())
    dict_to_be_exposed["repo_name_1"] = repo_name_1
    dict_to_be_exposed["bin_of_average_issue_closing_time_of_repo_1"] = \
        bin_of_average_issue_closing_time_of_repo_1.tolist()
    dict_to_be_exposed["repo_name_2"] = repo_name_2
    dict_to_be_exposed["bin_of_average_issue_closing_time_of_repo_2"] = \
        bin_of_average_issue_closing_time_of_repo_2.tolist()
    
    
    cluster.shutdown()
    return jsonify(dict_to_be_exposed)






# Expose issues closing times for a spacific repo by label
@app.route('/deep_insights_issues/issues_closing_times_by_label/<path:repo_name>', 
           methods=['GET'])
def get_issues_closing_times_by_label(repo_name):
    
    
    """
    Example JSON to be exposed:
    {
        "closing_times_per_label": [
            [
            "help wanted",
            168039959.0
            ],
            [
            "triaged: bug",
            168039959.0
            ],
            [
            "inactive",
            51647595.0
            ],
            [
            "triaged: question",
            51647595.0
            ],
            [
            "issue: question",
            701.0
            ]
        ]
    }
    """
    
    # Query Cassandra
    cassandra_container_name = 'cassandra_stelios'
    keyspace = 'prod_gharchive'
    cluster = Cluster([cassandra_container_name],port=9142)
    session = cluster.connect(keyspace)
    
    repo_unquoted = unquote(repo_name)
    prepared_query = f""\
        "SELECT repo_name, issue_number, label, opening_time, closing_time "\
        f"FROM {keyspace}.issue_closing_times_by_label "\
        f"WHERE repo_name = '{repo_unquoted}';"
    
    
    rows = session.execute(prepared_query)
    rows_list = rows.all()
    # Keep only closed (with non None closing times) issues
    rows_list = [row for row in rows_list if getattr(row, 'closing_time')!= None]
    
        
    # Map each issue closing time to a list with the corresponding label
    
    closing_times_per_issue_label = {}
    for row in rows_list:
        
        issue_label = getattr(row, 'label')
        # Initialize the list with the closing times of issues of a new label
        if issue_label not in closing_times_per_issue_label.keys():
            closing_times_per_issue_label[issue_label] = []
            
            
        opening_time_of_row = getattr(row, 'opening_time')
        closing_time_of_row = getattr(row, 'closing_time')
        # Remove rows containing closing time values earlier than opening time values
        try:
            opening_datetime = datetime.strptime(opening_time_of_row, '%Y-%m-%dT%H:%M:%SZ')
            closing_datetime = datetime.strptime(closing_time_of_row, '%Y-%m-%dT%H:%M:%SZ')
            time_diff = closing_datetime - opening_datetime
            closing_time_in_seconds = time_diff.total_seconds()
            if closing_time_in_seconds < 0:
                print(f"Repo name: {getattr(row, 'repo_name')}\n"
                    f"Issue number: {getattr(row, 'issue_number')}\n"
                    f"Closing time: {closing_datetime} is earlier than {opening_datetime}")
                
            # # Print closing times to get a sense of the data
            # # Print and verify the if the actual closing times 
            # # of the pull-requests match the ones some closing times    
            # print(f"Closing time: (days, seconds) = \
            #         {time_diff.days, time_diff.seconds}")
            # print(f"Link: github.com/{getattr(row, 'repo_name')}/pull/{getattr(row, 'pull_request_number')}")
            # print()
            # time.sleep(0.1)
  
        except Exception as e:
            print(f"Exception: {e}\n"
                f"Repo name: {getattr(row, 'repo_name')}"
                f"Issue number: {getattr(row, 'issue_number')}\n"
                f"Opening time of row: {opening_time_of_row}\n"
                f"Closing time of row: {closing_time_of_row}")

        
        # Add the closing time to the to the list
        closing_times_per_issue_label[issue_label].append(closing_time_in_seconds)
    
    
    average_closing_time_of_issues_per_label = {}
    for issue_label in closing_times_per_issue_label.keys():
        closing_times_list_given_label = closing_times_per_issue_label[issue_label]
        average_closing_time_of_issues_per_label[issue_label] = \
            np.average(closing_times_list_given_label)


    number_of_issues_per_label_dict = {}
    get_number_of_issues_per_label_query = "SELECT count(*) as number_of_issues_with_this_label, "\
        f"label FROM {keyspace}.issue_closing_times_by_label WHERE repo_name = '{repo_name}' "\
        "GROUP BY label;" 
    number_of_issues_per_label_rows = session.execute(get_number_of_issues_per_label_query)
    number_of_issues_per_label_rows = number_of_issues_per_label_rows.all()
    for number_of_issues_for_a_single_label in number_of_issues_per_label_rows:
        issue_label = getattr(number_of_issues_for_a_single_label, 'label')
        number_of_issues_for_the_label = getattr(number_of_issues_for_a_single_label, \
            'number_of_issues_with_this_label')
        number_of_issues_per_label_dict[issue_label] = number_of_issues_for_the_label
    
    
    def get_only_first_two_not_zero_times(num_of_seconds):
        """
        :param num_of_seconds: number of seconds to convert to higher time durations (years, months, etc)
        :return time_dict_keep_largest_two_non_zero_durations: The first two largest time durations that the seconds are converted into:
        
        Example:
        For input num_of_seconds = 168039959, we get 
        output: time_dict_keep_largest_two_non_zero_durations = {'years': 5, 'months': 3}
        """
        seconds_in_a_year = 365*24*60*60
        seconds_in_a_month = 30*24*60*60
        seconds_in_a_day = 24*60*60
        seconds_in_an_hour = 60*60
        seconds_in_a_minute = 60

        years, remaining_seconds = divmod(num_of_seconds, seconds_in_a_year)
        months, remaining_seconds = divmod(remaining_seconds, seconds_in_a_month)
        days, remaining_seconds = divmod(remaining_seconds, seconds_in_a_day)
        hours, remaining_seconds = divmod(remaining_seconds, seconds_in_an_hour)
        minutes, remaining_seconds = divmod(remaining_seconds, seconds_in_a_minute)
        seconds = remaining_seconds

        time_dict = {'year(s)': years, 'month(s)': months, 'day(s)':days, \
            'hour(s)':hours, 'minute(s)':minutes, 'second(s)':seconds}

        time_dict_keep_largest_two_non_zero_durations = {}

        # Iterate through the ordered time dictionary
        for k, v, in time_dict.items():
            if time_dict[k] != 0:
                time_dict_keep_largest_two_non_zero_durations[k] = v
            # Keep only 2 of the values in the 
            if len(time_dict_keep_largest_two_non_zero_durations.items()) == 2:
                break
            
        return time_dict_keep_largest_two_non_zero_durations

    average_closing_time_of_issues_per_label_list = []
    for issue_label in average_closing_time_of_issues_per_label.keys():
        
        total_num_of_seconds = average_closing_time_of_issues_per_label[issue_label]
        
        time_dict_largest_two_durations = get_only_first_two_not_zero_times(total_num_of_seconds)
        
        # days_hours_min_secs_tuple = 
        dict_element = {"issue_label" : issue_label, 
                "number_of_issues_with_this_label": number_of_issues_per_label_dict[issue_label],
                "average_closing_time_in_seconds": average_closing_time_of_issues_per_label[issue_label],
                "average_closing_time_formatted": time_dict_largest_two_durations}
        average_closing_time_of_issues_per_label_list.append(dict_element)


    average_closing_time_of_issues_per_label_list_sorted = \
        sorted(average_closing_time_of_issues_per_label_list, \
            key= lambda item: item["number_of_issues_with_this_label"], reverse=True)
    
    # dict_to_expose = \
    #     {"closing_times_per_label": average_closing_time_of_issues_per_label_sorted_list}
    # average_closing_time_of_issues_per_label_sorted_list = 0
    dict_to_expose = \
        {"repo_name": repo_name, \
            "closing_times_per_label": average_closing_time_of_issues_per_label_list_sorted}
    
    cluster.shutdown()
    # Expose the dictionary {label: closing_time in format (days, hours, minutes)}
    return jsonify(dict_to_expose)
# endregion
    

if __name__ == '__main__':
    
    app.run(host="0.0.0.0", port=3200)
    
    