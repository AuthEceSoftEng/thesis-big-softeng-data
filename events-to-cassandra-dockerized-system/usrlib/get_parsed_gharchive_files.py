import json, os

'''Demo to get some dummy parsed (not actually parsed) 
gharchive files of a given file'''


def save_files_parsed(files_parsed=dict, filepath_to_store_files_parsed=str):
    '''
    Save the state of the gharchive files parsed and are no longer needed 
    - ``files_parsed``: dictionary with the files_parsed. The filename is key and the
    value is the number of lines parsed from the file
    e.g. files_parsed = {'2024-04-01-0.json.gz':'end', '2024-04-01-1.json.gz':120}
    - ``filepath_to_store_files_parsed``: the filepath to store the 
    parsed files dictionary
    '''
    
    with open(filepath_to_store_files_parsed, "w") as new_state_file:
        # old_parsed_files_dict = restore_parsed_files(filepath_to_store_files_parsed)
        # old_parsed_file_line = old_parsed_files_dict[parsed_file]
        json.dump(files_parsed, new_state_file)
    
    
    # filename = os.path.basename(filepath_to_store_files_parsed)
    # print(f'Parsed lines of file {filename} were updated:')
    
    
    # Uncomment the following section to get all the files parsed and 
    # the line we left off in each one
    print(f'\nParsed lines of files were updated')
    for parsed_file in files_parsed.keys():
        print(parsed_file, ':', files_parsed[parsed_file])
    print()


def restore_parsed_files(parsed_files_filepath=str):
    '''
    Gets access to the files parsed to populate the Cassandra tables "repos" and "stats"
    ``parsed_files_filepath``: the filepath to the json file with the parsed files
    returns: ``files_parsed_dict`` which is a dict of the items in the parsed files
    '''
    if os.path.exists(parsed_files_filepath):
        with open(parsed_files_filepath, 'r') as parsed_files: 
            # If the parsed_files has no contents, print that the state is empty
            if  os.stat(parsed_files_filepath).st_size == 0:
                print("The producer has not parsed any files yet. Returning an empty dictionary")
                return {}
            else:
                dict_of_parsed_files = json.load(parsed_files)
    else: 
        raise ValueError('The state file for the parsed files does not exist. Cannot restore state')
    # files_parsed_dict = dict_of_parsed_files.items()
    # return files_parsed_dict
    return dict_of_parsed_files

# # The structure of the parsed files to store the state 
# files_parsed = {'2024-04-01-0.json.gz':11500, '2024-04-01-1.json.gz':2134, '2024-04-01-2.json.gz':52}
# filepath_to_store_state = '/home2/output/files_parsed.json'
# save_files_parsed(files_parsed, filepath_to_store_state)

# filepath_to_restore_state = filepath_to_store_state

# restored_parsed_files = restore_parsed_files(filepath_to_restore_state)
# print(restored_parsed_files)

# '''
# Send events to kafka
# Store up to what event was sent in restored_parsed_files
# '''

