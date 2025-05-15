
from cassandra.cluster import Cluster
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import time 
import json

# Get the data from cassandra
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

# Select to log transform data
log_transform_data = False
if log_transform_data == False:
    histogram_name = 'pull_requests_closing_times_histogram_original'
elif log_transform_data == True:
    histogram_name = 'pull_requests_closing_times_histogram_log_transformed'
else:
    raise Exception("'log_transform_data' should be set to True or False.")

    
get_pull_requests_histogram_info = f"SELECT bin_centers, bin_edges, abs_frequencies "\
    f"FROM {histogram_keyspace}.{histograms_table_name} WHERE histogram_name = '{histogram_name}';"        
row = session.execute(get_pull_requests_histogram_info)
row_in_response = row.one()

calculate_the_histogram_values_again = True

# Get the data if not already in keyspace 'histograms'
if row_in_response == None or calculate_the_histogram_values_again == True:
        
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
    
        print("Number of pull requests closing times in database: ", len(closing_times_list))
    
    
        num_of_histogram_bins = 20
        
        # Select to original or log scale data to remove skew
        if log_transform_data == False:
            closing_times_list_for_histogram = np.array(closing_times_list)
        elif log_transform_data == True:
            closing_times_list_for_histogram = np.log10(np.array(closing_times_list) + 1)
        else:
            raise Exception("'log_transform_data' should be set to True or False.")

        
        abs_frequencies, bin_edges = np.histogram(closing_times_list_for_histogram, 
                                                bins=num_of_histogram_bins)
        
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

# with open('/usrlib/matplotlib_plots/closing_times_list.txt', 'w') as file_object:
#     file_object.write(json.dumps(closing_times_list))

# Create histogram with various numbers of bins 
# and see which looks best


# number_of_first_bins_to_plot = 10
# abs_frequencies = abs_frequencies[0:number_of_first_bins_to_plot]
# bin_edges = bin_edges[0:number_of_first_bins_to_plot+1]
# print(f"abs_frequencies length: {len(abs_frequencies)}")
# print(f"bin_edges length: {len(bin_edges)}")

plt.stairs(abs_frequencies, bin_edges)
plt.savefig(f'/usrlib/matplotlib_plots/{histogram_name}.png')
time.sleep(3)





 