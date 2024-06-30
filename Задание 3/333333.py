import ray
import re

# initialization Ray
ray.init()


# Define a Ray task to process each line of the file
@ray.remote
def find_abbreviations(line):
    # Use regular expressions to match abbreviations such as pr., dr., etc.
    abbreviations = re.findall(r'\b[a-zA-Z]+\.', line)
    return abbreviations


def process_file(file_path):
    results = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            # Start a Ray task for each line of text
            result = find_abbreviations.remote(line)
            results.append(result)

    # Collect all results
    results = ray.get(results)

    # Print all recognized abbreviations
    for result in results:
        if result:
            print(result)


# Replace with your file path
file_path = 'your_file_path'
process_file(file_path)
