""" Gathers a random sample of emails from the enron data set using reservoir sampling.
"""
import email.parser
import random
import os 
from email.parser import Parser
from email.policy import default


""" Get a given number of files from the enron data set randomly.

Args:
    num_files (int): The number of files to return
    data_path (string): The path where the data is stored

Returns:
    dataframe: Of emails from enron dataset

Raises:
    ValueError: If num files is negative.
"""
def get_file_sample(num_files, data_path):
    if num_files < 0:
        raise ValueError("Number of files in sample must be greater than 0")
    
    return reservoir_sample(num_files, data_path)

"""Uses Reservoir Sampling to select a sample of files without needing to enumerate the number of files.

Args:
    num_files (int): The number of files to return.
    data_path (string): The path to the data in string for to be used in generate_file_stream 

Returns:
    dataframe: Of emails randomly sampled from the enron dataset.
"""
def reservoir_sample(num_files, data_path):
    reservoir = []
    for i, element in enumerate(generate_file_stream(data_path)):
        if i < num_files:
            reservoir.append(element)
        else:
            rand_int = random.randint(0, i)
            if rand_int < num_files:
                reservoir[rand_int] = element    
    return reservoir
    
"""Generates a stream of files to be used in the reservoir_sample function.

Args:
    data_path (string): A string that contains the top level directory where the data is formed

Yields:
    an iterator that walks through the nested directories from the top level directory.
"""
def generate_file_stream(data_path):
    for root, dirs, files in os.walk(data_path):
        for file in files:
            yield os.path.join(root, file)

"""Populates an array of file paths with the contents of those files.

Args:
    file_array (array): An array of file paths usually generated from get_file_sample

Returns:
    array: of the contents of each file.
"""
def populate_files(file_paths):
    msg_array = []
    for file_path in file_paths:
        with open(file_path, 'r') as fp:
            msg = Parser(policy=default).parse(fp)
            msg_array.append(msg)
    return msg_array


"""Processes an individual email and places it within a data frame breaking apart each individual portion

Ars:
    email_file (string): The raw string of an email containing information such as the sender, ID, email body, and 
    other arguments

Returns:
    dataframe: with the properly processed email.
"""
def process_email(email_file):
    return 

"""Reads the email into a dictionary with appropriately named 
"""
def process_email(email):
    return
def main():
    path = "C:\\Users\\sebtu\\Documents\\NLP Projects\\Enron\\data"
    files = get_file_sample(40, path)

if __name__ == '__main__':
    main()
