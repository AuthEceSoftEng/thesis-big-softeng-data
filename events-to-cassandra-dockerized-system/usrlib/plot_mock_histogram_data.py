
import matplotlib.pyplot as plt
import numpy as np
import time
import matplotlib.axes
import json

# # Generate random data for the histogram
# lambda_const = 10
# beta = 1/lambda_const
# sample_size = 20000
# data = np.random.exponential(scale=beta, size=sample_size) 
# plt.hist(data, bins=10, color='skyblue', edgecolor='black', log=True) 
# plt.xlabel('Values')
# plt.ylabel('Frequency')
# plt.title('Basic Histogram')
# plt.savefig('/home/xeon/thesis-big-softeng-data/events-to-cassandra-dockerized-system/usrlib/matplotlib_plots/exponential_hist_log_scale.png')
# # Wait a little fot the plot to be saved before closing the plot window
# plt.show()
# time.sleep(1)
# plt.close()

 
 
 
#  Original data
filepath_with_closing_times = '/home/xeon/thesis-big-softeng-data/events-to-cassandra-dockerized-system/usrlib/matplotlib_plots/closing_times_list.txt'
with open(filepath_with_closing_times, 'r') as closing_times_file:
    lines_in_file = closing_times_file.readlines()
    closing_times_list = json.loads(lines_in_file[0])
 
seconds_in_min = 60
seconds_in_hour = 60* seconds_in_min
seconds_in_day = 24* seconds_in_hour
seconds_in_month = 30* seconds_in_day
seconds_in_year = 365* seconds_in_day
custom_bin_edges = [0, seconds_in_min, seconds_in_hour, seconds_in_day, seconds_in_month, seconds_in_year, 10*seconds_in_year]
custom_bin_edges.append(max(closing_times_list))


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
abs_frequencies, _ = np.histogram(closing_times_list, custom_bin_edges)
custom_bin_edges_stringified = [period_to_string(seconds_to_period(custom_bin_edges[i])) for i in range(len(custom_bin_edges))]                                
tick_labels = ['{} - {}'.format(custom_bin_edges_stringified[i], custom_bin_edges_stringified[i+1]) for i in range(len(abs_frequencies))]

# tick_labels = None # Use of bin_centers as the tick labels
x_bar_coordinates = range(len(abs_frequencies))
bar_heights = abs_frequencies
ax.bar(x_bar_coordinates, height=bar_heights,  width=0.8, color='skyblue', edgecolor='black', align='center', tick_label=tick_labels)
plt.xticks(rotation=30)

ax.set_title('Number of pull requests given their closing times')
ax.set_ylabel('Absolute frequencies')
ax.set_xlabel('Time (sec)')
plt.savefig('./usrlib/matplotlib_plots/pull_requests_closing_times_original_data_local.png',  bbox_inches='tight')
plt.show()
time.sleep(0.5)
plt.close()
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
# # Not transformed data
# abs_frequencies = [1.0436e+05, 672, 358, 249, 133, 96, 54, 86, 20, 7, 7, 38, 1, 3, 0, 4, 1, 0, 0, 2] 
# bin_centers = [1.0233e+07, 3.07e+07, 5.1167e+07, 7.1634e+07, 9.2101e+07, 1.1257e+08, 1.3304e+08, 1.535e+08, 1.7397e+08, 1.9444e+08, 2.149e+08, 2.3537e+08, 2.5584e+08, 2.763e+08, 2.9677e+08, 3.1724e+08, 3.3771e+08, 3.5817e+08, 3.7864e+08, 3.9911e+08]
# bin_edges = [0, 2.0467e+07, 4.0934e+07, 6.1401e+07, 8.1868e+07, 1.0233e+08, 1.228e+08, 1.4327e+08, 1.6374e+08, 1.842e+08, 2.0467e+08, 2.2514e+08, 2.456e+08, 2.6607e+08, 2.8654e+08, 3.07e+08, 3.2747e+08, 3.4794e+08, 3.6841e+08, 3.8887e+08, 4.0934e+08]


# fig, ax = plt.subplots()
# ax.bar(bin_centers, abs_frequencies, width=[bin_edges[i+1] - bin_edges[i] for i in range(len(bin_centers))], color='skyblue', edgecolor='black')
# ax.set_title('Number of pull requests given their closing times')
# ax.set_ylabel('Absolute frequencies')
# ax.set_xlabel('Time (sec)')
# # def seconds_to_log(seconds):   
# #     seconds = np.clip(seconds, a_min=0, a_max=None)
# #     log_val = np.log10(seconds + 1)
# #     return log_val
# # def log_to_seconds(log_val):
# #     seconds = np.power(10, log_val) - 1
# #     return seconds
# # sec_ax_x = ax.secondary_xaxis('top', functions=(seconds_to_log, log_to_seconds))
# # sec_ax_x.set_xlabel(r'$\log (seconds + 1)$')
# plt.savefig('./usrlib/matplotlib_plots/pull_requests_closing_times.png')
# plt.show()
# # Wait a little fot the plot to be saved before closing the plot window
# time.sleep(1)
# plt.close()




# # # Log transformed data
# abs_frequencies = [216, 7388, 18515, 8450, 9891, 6649, 5445, 4785, 4825, 5124, 4623, 7274, 4166, 5817, 6911, 2886, 1423, 908, 648, 151]
# bin_centers = [0.215302, 0.645906, 1.07651, 1.50711, 1.93772, 2.36832, 2.79893, 3.22953, 3.66014, 4.09074, 4.52134, 4.95195, 5.38255, 5.81316, 6.24376, 6.67437, 7.10497, 7.53557, 7.96618, 8.39678]
# bin_edges = [0, 0.430604, 0.861208, 1.29181, 1.72242, 2.15302, 2.58363, 3.01423, 3.44483, 3.87544, 4.30604, 4.73665, 5.16725, 5.59785, 6.02846, 6.45906, 6.88967, 7.32027, 7.75088, 8.18148, 8.61208]

# fig, ax = plt.subplots()
# ax.bar(bin_centers, abs_frequencies, width=[bin_edges[i+1] - bin_edges[i] for i in range(len(bin_centers))], color='skyblue', edgecolor='black')
# ax.set_title('Number of pull requests given their closing times')
# ax.set_ylabel('Absolute frequencies')
# ax.set_xlabel(r'$\log (seconds + 1)$')
# def seconds_to_log(seconds):   
#     seconds = np.clip(seconds, a_min=0, a_max=None)
#     log_val = np.log10(seconds + 1)
#     return log_val
# def log_to_seconds(log_val):
#     seconds = np.power(10, log_val) - 1
#     return seconds
# sec_ax_x = ax.secondary_xaxis('top', functions=(log_to_seconds, seconds_to_log))
# sec_ax_x.set_xlabel('Time (sec)')
# plt.savefig('./usrlib/matplotlib_plots/pull_requests_closing_times_log_transformed.png')
# plt.show()
# # Wait a little fot the plot to be saved before closing the plot window
# time.sleep(1)
# plt.close()




# Plot and style it to make it readable
# Add second axis using the inverse transform
# Add custom labels (seconds into one and days/months/years to the secondary)

