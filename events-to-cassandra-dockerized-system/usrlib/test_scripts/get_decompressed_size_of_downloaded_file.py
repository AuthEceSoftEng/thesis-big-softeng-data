import os
import math
import subprocess
import sys

def bytes_to_mb(file_size_in_bytes):
    # bytes_in_mb = math.pow(1024, 2) # for MiB (MibiBytes)
    bytes_in_mb = math.pow(1000, 2) # for MB (MegaBytes)
    mb_in_file = round(file_size_in_bytes/bytes_in_mb, 1)
    return mb_in_file

def get_compressed_file_size(filepath):
    return os.path.getsize(filepath)

def get_decompressed_file_size(compressed_filepath):
    """
    Get the decompressed file size of a compressed file given its filepath
    """
    get_size_decompressed_command = f'gzip -dc {compressed_filepath} | wc -c'
    command_output =  \
        subprocess.run(get_size_decompressed_command, shell=True, capture_output=True)
    command_stdout = command_output.stdout
    size_in_bytes_decompressed = int(command_stdout.decode('utf-8').rstrip())
    return size_in_bytes_decompressed


def get_compression_ratio(compressed_bytes, decompressed_bytes):
    return round(decompressed_bytes/compressed_bytes, 1)

# # Demo 1: Compressed and uncompressed file size (in B and MB) and compression ratio
# region

# # Compressed size in bytes
# filepath = '/home/xeon/thesis-big-softeng-data/events-to-cassandra-dockerized-system/usrlib/github_data/2024-12-01-1-thinned.json.gz'
# size_in_bytes_compressed = get_compressed_file_size(filepath)
# print(f"size_in_bytes_compressed: {size_in_bytes_compressed} B")

# # Compressed size in MBs
# size_in_mb_compressed = bytes_to_mb(size_in_bytes_compressed)
# print(f"size_in_mb_compressed: {size_in_mb_compressed} MB")


# # Decompressed size in bytes
# size_in_bytes_decompressed = get_decompressed_file_size(filepath)
# print(f"size_in_bytes_decompressed: {size_in_bytes_decompressed} B")

# # Decompressed size in MBs
# size_in_mb_decompressed = bytes_to_mb(size_in_bytes_decompressed)
# print(f"size_in_mb_decompressed: {size_in_mb_decompressed} MB")

# # Compression ratio
# print(f"Compression ratio of {os.path.basename(filepath)}: {get_compression_ratio(size_in_bytes_compressed, size_in_bytes_decompressed)}")

# endregion

# # Demo 2: Print in stdout compressed and decompressed size in MB and compression ratio of all files in a directory 
# # region
# dirpath = '/home/xeon/thesis-big-softeng-data/events-to-cassandra-dockerized-system/usrlib/github_data/'
# dir_filenames = sorted(os.listdir(dirpath))
# num_of_files_to_iterate_through = 5
# # num_of_files_to_iterate_through = len(dir_filenames) 
# total_compressed_size_in_mb = 0
# total_decompressed_size_in_mb = 0
# total_compression_ratio = 0

# file_header_stdout = "File\t\t\t\tCompressed Size (MB)\tDecompressed Size (MB)\tCompression ratio"
# print(file_header_stdout)


# for i in range(num_of_files_to_iterate_through):
#     filename = dir_filenames[i]
#     filepath = os.path.join(dirpath, filename)
#     compressed_file_size_in_bytes = os.path.getsize(filepath)
#     compressed_file_size_in_mb = bytes_to_mb(compressed_file_size_in_bytes)
#     decompressed_file_size_in_bytes = get_decompressed_file_size(filepath)
#     decompressed_file_size_in_mb = bytes_to_mb(get_decompressed_file_size(filepath))
#     compression_ratio = get_compression_ratio(compressed_file_size_in_bytes, decompressed_file_size_in_bytes)
    
#     total_compressed_size_in_mb += compressed_file_size_in_bytes
#     total_decompressed_size_in_mb += decompressed_file_size_in_bytes
#     total_compression_ratio += compression_ratio
    
#     decompressed_file_row_stdout = f"{filename}\t{compressed_file_size_in_mb}\t\t\t{decompressed_file_size_in_mb}\t\t\t{compression_ratio}"
#     print(decompressed_file_row_stdout)
      

# print(file_header_stdout)
# separator_string = "-----------------------------------------------------------------------------------------------"
# print(separator_string)

# average_compressed_size_in_mb = round(bytes_to_mb(total_compressed_size_in_mb/num_of_files_to_iterate_through), 1)
# average_decompressed_size_in_mb = round(bytes_to_mb(total_decompressed_size_in_mb/num_of_files_to_iterate_through), 1)
# average_compression_ratio = round(total_compression_ratio/num_of_files_to_iterate_through, 1)
# averages_row_stdout = f"Averages\t\t\t{average_compressed_size_in_mb}\t\t\t{average_decompressed_size_in_mb}\t\t\t{average_compression_ratio}"
# print(averages_row_stdout)

# # endregion



# Demo 3: Write in file compressed and decompressed size in MB and compression ratio of all files in a directory 
# region


dirpath = '/home/xeon/thesis-big-softeng-data/events-to-cassandra-dockerized-system/usrlib/github_data/'
dir_filenames = sorted(os.listdir(dirpath))
num_of_files_to_iterate_through = 5
# num_of_files_to_iterate_through = len(dir_filenames) 
total_compressed_size_in_mb = 0
total_decompressed_size_in_mb = 0
total_compression_ratio = 0
filepath_to_write_compression_ratios_of_files_into = \
    '/home/xeon/thesis-big-softeng-data/events-to-cassandra-dockerized-system/usrlib/test_scripts/decompressed_file_sizes_for_testing.txt'

print("Started adding files sizes and compression ratios into files\n")


with open(filepath_to_write_compression_ratios_of_files_into, 'a') as decompressed_sizes_file:
    
    file_header_to_write_in_file = "File\t\t\t\t\t\t\tCompressed Size (MB)\tDecompressed Size (MB)\tCompression ratio"
    decompressed_sizes_file.write(file_header_to_write_in_file + "\n")
    for i in range(num_of_files_to_iterate_through):
        filename = dir_filenames[i]
        if not filename.endswith('thinned.json.gz'):
            continue
        filepath = os.path.join(dirpath, filename)
        compressed_file_size_in_bytes = os.path.getsize(filepath)
        compressed_file_size_in_mb = bytes_to_mb(compressed_file_size_in_bytes)
        decompressed_file_size_in_bytes = get_decompressed_file_size(filepath)
        decompressed_file_size_in_mb = bytes_to_mb(get_decompressed_file_size(filepath))
        compression_ratio = get_compression_ratio(compressed_file_size_in_bytes, decompressed_file_size_in_bytes)
        total_compressed_size_in_mb += compressed_file_size_in_bytes
        total_decompressed_size_in_mb += decompressed_file_size_in_bytes
        total_compression_ratio += compression_ratio
        
        decompressed_file_row_to_write_in_file = f"{filename}\t{compressed_file_size_in_mb}\t\t\t\t\t\t{decompressed_file_size_in_mb}\t\t\t\t\t{compression_ratio}"
        decompressed_sizes_file.write(decompressed_file_row_to_write_in_file + "\n")
        
        
        sys.stdout.write(f"\033[F\033[KNumber of files' sizes written: {i}/{num_of_files_to_iterate_through}.\n"\
            f"Writing sizes for file '{filename}'")
        sys.stdout.flush()
        
    print()
    
    decompressed_sizes_file.write(file_header_to_write_in_file + "\n")
    separator_string = "-----------------------------------------------------------------------------------------------"
    decompressed_sizes_file.write(separator_string + "\n")
    
    average_compressed_size_in_mb = round(bytes_to_mb(total_compressed_size_in_mb/num_of_files_to_iterate_through), 1)
    average_decompressed_size_in_mb = round(bytes_to_mb(total_decompressed_size_in_mb/num_of_files_to_iterate_through), 1)
    average_compression_ratio = round(total_compression_ratio/num_of_files_to_iterate_through, 1)
    averages_row_to_write_in_file = f"Averages\t\t\t\t\t\t{average_compressed_size_in_mb}\t\t\t\t\t\t{average_decompressed_size_in_mb}\t\t\t\t\t{average_compression_ratio}"
    decompressed_sizes_file.write(averages_row_to_write_in_file + "\n\n")

# endregion