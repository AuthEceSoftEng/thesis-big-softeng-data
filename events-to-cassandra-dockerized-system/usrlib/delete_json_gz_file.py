import os
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from configparser import ConfigParser


# Parse the command line.
parser = ArgumentParser()
parser.add_argument('filepath', type=str)
args = parser.parse_args()

filepath_of_file_to_delete = args.filepath

if os.path.exists(filepath_of_file_to_delete):
    print(f"File {filepath_of_file_to_delete} exists.\n Deleting...\n")
    os.remove(filepath_of_file_to_delete)
    print(f"File {filepath_of_file_to_delete} deleted successfully")
else:
    print(f"File {filepath_of_file_to_delete} does not exist.\nCannot be deleted\n")
    