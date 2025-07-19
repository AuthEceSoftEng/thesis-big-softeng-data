import json, gzip, os, sys
import warnings

# from produce_from_last_line_of_file import extract_compressed_file_from_path
# import get_parsed_gharchive_files import save_files_parsed, restore_parsed_files


from collections import defaultdict

glob_counter = 0


def heavy_process_json_event(event):
	"""
	Function that receives as input an event and keeps only fields of interest.
	"""
	
	
 
	new_event = defaultdict(dict)
	try:
		new_event = {
      		"id": event["id"],
			"type": event["type"],	
   			"actor": event["actor"]["login"],
			"repo": {
       			"full_name": event["repo"]["name"]
          	},
			"created_at": event["created_at"]
		}
		if new_event["type"] == "PushEvent":
			new_event["payload"] = {
				"distinct_size": event["payload"]["distinct_size"],
    			"size": event["payload"]["size"],
			}		
			# print(new_event)
			# raise Exception("Check the event payload")
		elif new_event["type"] == "PullRequestEvent":
			new_event["payload"] = {
				"action": event["payload"]["action"],
				"pull_request": {
					"number": event["payload"]["pull_request"]["number"],
					"user": event["payload"]["pull_request"]["user"]["login"],				
     				"commits": event["payload"]["pull_request"]["commits"],
					"created_at": event["payload"]["pull_request"]["created_at"],
     				"merged_at": event["payload"]["pull_request"]["merged_at"],
					"closed_at": event["payload"]["pull_request"]["closed_at"]
				}
			}
			# print(new_event)
			# raise Exception("Check the event payload")
		elif new_event["type"] == "IssuesEvent":
			new_event["payload"] = {
				"action": event["payload"]["action"],
				"issue": {
					"number": event["payload"]["issue"]["number"],
					"created_at": event["payload"]["issue"]["created_at"],
					"closed_at": event["payload"]["issue"]["closed_at"],
					# "labels": event["payload"]["issue"]["labels"]
     				"labels": [label_element["name"] for label_element in event["payload"]["issue"]["labels"]]
				}		
			}
			# print(new_event)
			# raise Exception("Check the event payload")	
   
	except KeyError as e:
		raise KeyError(e)
  
	return new_event

 



warnings.filterwarnings("ignore", category=DeprecationWarning) 



def heavy_thin_data_of_file(input_filepath, output_filepath, do_again=False, delete_original_file=True):
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
	

	number_of_lines_thinned_per_print = 100000

	if not os.path.exists(output_filepath) or do_again == True:
		print(f'Thinning events of {os.path.basename(input_filepath)}...')
      	# Calculate size of file (number of lines of file)
		with gzip.open(input_filepath, 'r') as file_object:
			linesInFile = len(file_object.readlines())

		with gzip.open(output_filepath, 'wb') as outstream:
			with gzip.open(input_filepath) as instream:
				for i, line in enumerate(instream):
					event = json.loads(line)
					# print(f"Original event repo field: {event['repo']}")
					# print(event["repo"]["full_name"])
					thinned_event = heavy_process_json_event(event)	
					thinned_line = json.dumps(thinned_event) + '\n'
					outstream.write(thinned_line.encode())
					# Print every e.g. 1000 lines thinned
					if i % number_of_lines_thinned_per_print == 0:
						sys.stdout.write("\r JSON events thinned: {0}/{1}".format(i+1, linesInFile))
						sys.stdout.flush()
						# if i == 10000:	
						# 	# print(f"Thinned event repo field: {thinned_event['repo']}")
						# 	break	
				# Print again at the last line thinned			
				sys.stdout.write("\r JSON events thinned: {0}/{1}".format(linesInFile, linesInFile))
				sys.stdout.flush()
		print('...Done')	
 						
	elif os.path.exists(output_filepath) and do_again == False:
		print(f"{os.path.basename(output_filepath)} with the thinned events already exists.")
	
	
	if os.path.exists(input_filepath) and delete_original_file == True:
		os.remove(input_filepath)	
		print(f"Deleted original file: '{os.path.basename(input_filepath)}'")
  
