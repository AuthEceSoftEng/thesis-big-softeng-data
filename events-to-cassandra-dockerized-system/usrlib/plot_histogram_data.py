from cassandra.cluster import Cluster
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import time

# Create and save the pull requests or issues histogram
create_pull_requests_graph = True
calculate_the_histogram_values_again = False

if create_pull_requests_graph == True:
    # Pull requests
    histogram_name = 'pull_requests_closing_times_histogram_custom_edges'    
    closing_times_table_name = 'pull_request_closing_times'
else:
    # Issues
    histogram_name = 'issues_closing_times_histogram_custom_edges'    
    closing_times_table_name = 'issue_closing_times'


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

# Query histogram info (bin centers, edges and absolute frequencies) for the given histogram name if exists
query_histogram_info = f"SELECT bin_centers, bin_edges, abs_frequencies "\
    f"FROM {histograms_keyspace}.{histograms_table_name} WHERE histogram_name = '{histogram_name}';"        
cluster = Cluster([cassandra_host],port=cassandra_port)
session = cluster.connect()    
row = session.execute(query_histogram_info)
row_in_response = row.one()


# Calculate histogram info (bin centers, edges and absolute frequencies) if its name was not found in the database (or if you want to recalculate it the process)
if row_in_response == None or calculate_the_histogram_values_again == True:
        
        # Query all repos closing times
        keyspace = "prod_gharchive"
        print(f"Bin centers, edges and absolute values of histogram '{histogram_name}' are not in "\
            f"table '{histograms_keyspace}.{histograms_table_name}'.\n"\
            f"Calculating based on table {keyspace}.{closing_times_table_name}...")
        
        if create_pull_requests_graph == True:
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
        else:
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


        def calculate_histogram_info(bin_edges=list, closing_times_list=list):
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
    
        bin_centers, abs_frequencies = calculate_histogram_info(bin_edges, closing_times_list)            
            
        def store_histogram_info_in_cassandra(cassandra_host, cassandra_port, histograms_keyspace, histograms_table_name, histogram_name, bin_centers, bin_edges, abs_frequencies):
            
            cluster = Cluster([cassandra_host],port=cassandra_port)
            session = cluster.connect()    
            # Insert bin centers, bin edges and absolute frequencies in cassandra
            insert_histogram_info = f"INSERT INTO {histograms_keyspace}.{histograms_table_name} "\
                f"(histogram_name, bin_centers, bin_edges, abs_frequencies) VALUES ('{histogram_name}', {bin_centers}, {bin_edges}, "\
                    f"{abs_frequencies});"
            session.execute(insert_histogram_info) 

        store_histogram_info_in_cassandra(cassandra_host, cassandra_port, histograms_keyspace, histograms_table_name, histogram_name, bin_centers, bin_edges, abs_frequencies) 
            
        
elif row_in_response != None:
    bin_centers = getattr(row_in_response, 'bin_centers')
    bin_edges = getattr(row_in_response, 'bin_edges')
    abs_frequencies = getattr(row_in_response, 'abs_frequencies')
    print(f"Bin centers, bin edges and absolute frequencies of histogram '{histogram_name}' already exist in table {histograms_keyspace}.{histograms_table_name}\n")



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
            time_dict_keep_largest_two_non_zero_durations[k] = v
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


fig, ax = plt.subplots()
custom_bin_edges_stringified = [period_to_string(seconds_to_period(bin_edges[i])) for i in range(len(bin_edges))]                                
tick_labels = ['{} - {}'.format(custom_bin_edges_stringified[i], custom_bin_edges_stringified[i+1]) for i in range(len(abs_frequencies))]

# tick_labels = None # Use of bin_centers as the tick labels
x_bar_coordinates = range(len(abs_frequencies))
bar_heights = abs_frequencies
ax.bar(x_bar_coordinates, height=bar_heights,  width=0.8, color='skyblue', edgecolor='black', align='center', tick_label=tick_labels)
plt.xticks(rotation=30)

ax.set_title(f"Bar graph of '{histogram_name}'")
ax.set_ylabel('Absolute frequencies')
ax.set_xlabel('Time')
plt.savefig(f'/usrlib/matplotlib_plots/{histogram_name}.png',  bbox_inches='tight')
time.sleep(1)






 