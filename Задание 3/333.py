import ray
import os

# initialization Ray
ray.init()

# Define functions to process a single row of data
@ray.remote
def process_line(line):
    # Separate components in each row
    parts = line.split('<tabs>')
    if len(parts) < 3:
        return [], 0  # If the format is incorrect, returns an empty list and a length of 0

    # Extract the body part, split the words
    content = parts[2].strip()
    words = content.split()
    lengths = [len(word) for word in words]
    return lengths, len(words)

# Reading and processing files
def process_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        # Create a list of tasks, with each row of data corresponding to a task
        futures = [process_line.remote(line) for line in file]

    # Collection of processing results
    results = ray.get(futures)

    # Calculate total length and total number of words
    total_length = sum(sum(lengths) for lengths, count in results)
    total_count = sum(count for lengths, count in results)

    # Calculate average word length
    if total_count > 0:
        average_length = total_length / total_count
        return average_length
    else:
        return 0

# Replace the following path with your file path
file_path = 'D:\Задание 3\wiki.txt'
average_length = process_file(file_path)
print(f"Average word length: {average_length:.2f}")




