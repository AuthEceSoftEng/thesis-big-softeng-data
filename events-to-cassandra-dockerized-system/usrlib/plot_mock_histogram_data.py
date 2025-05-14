
import matplotlib.pyplot as plt
import numpy as np
import time



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

 
 
# Original data
abs_frequencies = [1.0436e+05, 672, 358, 249, 133, 96, 54, 86, 20, 7, 7, 38, 1, 3, 0, 4, 1, 0, 0, 2] 
bin_centers = [1.0233e+07, 3.07e+07, 5.1167e+07, 7.1634e+07, 9.2101e+07, 1.1257e+08, 1.3304e+08, 1.535e+08, 1.7397e+08, 1.9444e+08, 2.149e+08, 2.3537e+08, 2.5584e+08, 2.763e+08, 2.9677e+08, 3.1724e+08, 3.3771e+08, 3.5817e+08, 3.7864e+08, 3.9911e+08]
bin_edges = [0, 2.0467e+07, 4.0934e+07, 6.1401e+07, 8.1868e+07, 1.0233e+08, 1.228e+08, 1.4327e+08, 1.6374e+08, 1.842e+08, 2.0467e+08, 2.2514e+08, 2.456e+08, 2.6607e+08, 2.8654e+08, 3.07e+08, 3.2747e+08, 3.4794e+08, 3.6841e+08, 3.8887e+08, 4.0934e+08]



# Log transformed data
abs_frequencies = [216, 7388, 18515, 8450, 9891, 6649, 5445, 4785, 4825, 5124, 4623, 7274, 4166, 5817, 6911, 2886, 1423, 908, 648, 151]
bin_centers = [0.215302, 0.645906, 1.07651, 1.50711, 1.93772, 2.36832, 2.79893, 3.22953, 3.66014, 4.09074, 4.52134, 4.95195, 5.38255, 5.81316, 6.24376, 6.67437, 7.10497, 7.53557, 7.96618, 8.39678]
bin_edges = [0, 0.430604, 0.861208, 1.29181, 1.72242, 2.15302, 2.58363, 3.01423, 3.44483, 3.87544, 4.30604, 4.73665, 5.16725, 5.59785, 6.02846, 6.45906, 6.88967, 7.32027, 7.75088, 8.18148, 8.61208]


