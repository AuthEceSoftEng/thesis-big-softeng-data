import json, gzip, os, sys
import warnings

from produce_from_last_line_of_file import extract_compressed_file_from_path
# import get_parsed_gharchive_files import save_files_parsed, restore_parsed_files

glob_counter = 0

def process_json_event(event):
	# global glob_counter 
	# glob_counter +=1
	# print(f"Got in process_json_event {glob_counter} times")
	"""
	Recursive function that receives as input an event and cleans it up.
	"""
	if isinstance(event, dict):
		new_event = {}
		for k, v in event.items():
			# From field "repo", keep "name", "language" and "topics"
			if isinstance(v, dict) and (k == "repo"):
				if "full_name" in v.keys():
					new_event[k] = {"full_name": v["full_name"]}
					# continue	
				# If "full_name" is in repo's fields, use that, else use field "name" 
				elif "name" in v.keys():
					new_event[k] = {"full_name": v["name"]}
					# continue
				if "language" in v.keys() and "topics" in v.keys():
					new_event[k]["language"] =  v["language"]
					new_event[k]["topics"] =  v["topics"]
					# continue
				elif "language" in v.keys():
					new_event[k]["language"] =  v["language"]
					# continue
				elif "topics" in v.keys():
					new_event[k]["topics"] =  v["topics"]
					# continue
				continue
					
			# From fields "actor" (or whatever signifies a user) and from "org", keep "login"
			if isinstance(v, dict) and "login" in v and (k == "actor" or k == "user" or \
       				k == "owner" or k == "author" or k == "creator" or k == "assignee" or k == "uploader" or k == "org"): 
				new_event[k] = v["login"]
				continue
		
			# Remove all links
			if k == "_links" or (isinstance(v, str) and v.startswith('https://')): 
				continue
			
			# Remove all release assets	
			if k == "assets": 
				continue
			
			# Keep the first 100 characters of all diff hunks (e.g. pull request comments), 
			# bodies (e.g. pull requests) or messages (e.g. commits), 
			# descriptions (e.g. issue comments), short html description (e.g. releases)
			if (k == "diff_hunk" or k == "body" or k == "message" or \
       				k == "commit_message" or k == "description" or \
               		k == "short_description_html") and v != None: 
				# Ignore non ascii characters
				new_event[k] = v[:100].encode('ascii', 'ignore').decode('unicode-escape', 'ignore') 
				continue
			
			# Change all commit authors from objects to names
			# Escape non-ascii characters			
			if isinstance(v, dict) and "name" in v and k == "author" and v!= None: 
				new_event[k] = v["name"]
				continue

			# Change all users and orgs from objects to names
			if isinstance(v, list) and (k == "assignees" or k == "requested_reviewers"): 
				new_event[k] = [elem["login"] for elem in v]
				continue

			# Change all labels from objects to names
			if isinstance(v, list) and k == "labels": 
				new_event[k] = [elem["name"] for elem in v]
				continue

			# Change all licenses from objects to names
			if isinstance(v, dict) and (k == "license"): 
				new_event[k] = v["name"]
				continue
			new_event[k] = process_json_event(v)
		return new_event
	elif isinstance(event, list):
		return [process_json_event(elem) for elem in event]
	else:
		return event


warnings.filterwarnings("ignore", category=DeprecationWarning) 



def thin_data_of_file(input_filepath, output_filepath, do_again=False):
	'''
	Thins the events of the gharchive filepath `input_filepath` 
	and stores it into `output_filepath`.
	If the do_again is True, the decompression is done again and 
	the existing file is overwritten
 
	`input_filepath`: GHArchive file with events to thin data
	`output_filepath`: GhArchive file with thinned events 
	`do_again`: In case the GHArchuive file with the thinned events
	already exists, thin the events again and overwrite the 
	output_filepath contents
	'''
	# Calculate size of file (number of lines of file)
	with gzip.open(input_filepath, 'r') as file_object:
			linesInFile = len(file_object.readlines())

	number_of_lines_thinned_per_print = 1000

	print(f'Thinning events of {os.path.basename(input_filepath)}...')
	if not os.path.exists(output_filepath) or do_again == True:
		with gzip.open(output_filepath, 'wb') as outstream:
			with gzip.open(input_filepath) as instream:
				for i, line in enumerate(instream):
					event = json.loads(line)
					# print(f"Original event repo field: {event['repo']}")
					# print(event["repo"]["full_name"])
					thinned_event = process_json_event(event)	
					thinned_line = json.dumps(thinned_event) + '\n'
					outstream.write(thinned_line.encode())
					# Print every e.g. 1000 lines thinned
					if i % number_of_lines_thinned_per_print == 0:
						sys.stdout.write("\r JSON events thinned: {0}/{1}".format(i+1, linesInFile))
						sys.stdout.flush()
						# if i == 17:	
							# 	print(f"Thinned event repo field: {thinned_event['repo']}")
							# 	break	
				# Print again at the last line thinned			
				sys.stdout.write("\r JSON events thinned: {0}/{1}".format(linesInFile, linesInFile))
				sys.stdout.flush()
							
	elif os.path.exists(output_filepath) and do_again == False:
		print(f"{os.path.basename(output_filepath)} with the thinned events already exists.")
	print('...Done')
  
# # Demo
# in_dir_path = '/home2/input/2024-01-01/'
# input_filename = '2024-01-01-0.json.gz'
# input_path = os.path.join(in_dir_path, input_filename)

# out_dir_path = '/home2/other_data'
# output_filename = '2024-01-01-0-thinned.json.gz'
# output_path = os.path.join(out_dir_path, output_filename)


# thin_data_of_file(input_path, output_path, do_again=True)
# extract_compressed_file_from_path(output_path, do_again=True)