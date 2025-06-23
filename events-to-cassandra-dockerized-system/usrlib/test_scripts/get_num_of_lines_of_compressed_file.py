import os
import math
import subprocess
import sys



# Get number of lines in a compressed file given filepath
def get_number_of_lines_in_file(filepath):
    get_number_of_lines_of_decompressed_file_command = \
        f'zcat  {filepath} | wc -l'
    command_output =  \
        subprocess.run(get_number_of_lines_of_decompressed_file_command,
                    shell=True, capture_output=True)
    command_stdout = command_output.stdout
    num_of_lines_in_file = int(command_stdout.decode('utf-8').rstrip())
    return num_of_lines_in_file

# Demo 3: Write in file compressed and decompressed size in MB and compression ratio of all files in a directory 
# region

if __name__ == '__main__':
    
    dirpath = '/github_data'
    dir_filenames = sorted(os.listdir(dirpath))
    # num_of_files_to_iterate_through = 5
    num_of_files_to_iterate_through = len(dir_filenames) 
    filepath_to_write_number_of_lines_of_files_into = \
        '/test_scripts/number_of_lines_in_files.txt'

    total_number_on_lines = 0
    
    
    print("Started adding files sizes and compression ratios into files\n")


    with open(filepath_to_write_number_of_lines_of_files_into, 'a') as num_of_lines_file_output:
        
        file_header_to_write_in_file = "File\t\t\t\t\t\t\t\tNumber of lines"
        num_of_lines_file_output.write(file_header_to_write_in_file + "\n")
        
        for i in range(num_of_files_to_iterate_through):
            filename = dir_filenames[i]
            if not filename.endswith('thinned.json.gz') or not filename.startswith('2024-12-'):
                continue
            filepath = os.path.join(dirpath, filename)
            filename = os.path.basename(filepath)
            num_of_lines_in_file = get_number_of_lines_in_file(filepath)
            row_to_write_in_file = f"{filename}\t\t{num_of_lines_in_file}"
            num_of_lines_file_output.write(row_to_write_in_file + "\n")
            
            total_number_on_lines += num_of_lines_in_file
            
            sys.stdout.write(f"\033[F\033[KNumber of files' sizes written: {i+1}/{num_of_files_to_iterate_through}.\n"\
                f"Writing sizes for file '{filename}'")
            sys.stdout.flush()
            
        print()
        
        num_of_lines_file_output.write(file_header_to_write_in_file + "\n")
        separator_string = "-----------------------------------------------------"
        num_of_lines_file_output.write(separator_string + "\n")
        
        num_of_lines_file_output.write(f"Total number of lines\t\t\t\t{total_number_on_lines}" + "\n")
        average_num_of_lines = math.floor(total_number_on_lines/num_of_files_to_iterate_through)
        num_of_lines_file_output.write(f"Average number of lines\t\t\t\t{average_num_of_lines}" + "\n")
        num_of_lines_file_output.write(separator_string + "\n")

        
        

    # endregion