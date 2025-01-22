'''
Open all files in input_folder and thin its data and put it into a output folder 
(and TODO: delete the original input data)
'''

import os
import time
import sys

# sys.path.append('/home/xeon/Thesis/local-kafka-flink-cassandra-integration/presentation-10-demo/task-2-store-tables-repos-and-stats')
# sys.path.append('/home/xeon/Thesis/local-kafka-flink-cassandra-integration/presentation-10-demo/task-2-store-tables-repos-and-stats')

from thin_data import thin_data_of_file
# print(sys.path)
# exit()

# import 'task-2-store-tables-repos-and-stats' as task_2



def thin_data_of_folder(input_folder, output_folder, do_again=False):
    '''
    Thins gharchive files' events keeping only json fields we need 
    '''
    # If input folder does not exist, print appropriate message
    if not os.path.exists(input_folder):
        raise ValueError(f'Input folder {os.path.basename(input_folder)}'
            'does not exist\nYou can create it using '
            'script download_gharchive_in_period.py')
    
    
    # If the output folder does not exist, create it
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    base_folder = os.path.basename((input_folder))
    for i in range(24):
        
        # Construct the file names
        input_filename = base_folder + '-' + str(i) + '.json.gz'
        input_filepath = os.path.join(input_folder, input_filename)
        
        # If there are no more files to compress in the folder, stop execution
        if not os.path.exists(input_filepath):
            break
        
        output_filepath = os.path.join(output_folder, input_filename)

        # Compress the data one by one
        if not os.path.exists(output_filepath) or do_again==True:    
            thin_data_of_file(input_filepath, output_filepath, do_again)
        else:
            print(f"Compressed file {output_filepath} already exists")


# Demo
input_folder = '/home2/input/2024-01-02'
output_folder = '/home2/output/2024-01-02'

st = time.time()
thin_data_of_folder(input_folder, output_folder)
et = time.time()
print(f"Time elapsed: {et-st} seconds")