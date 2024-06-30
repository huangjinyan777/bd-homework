import ray
import re

# initialization Ray
ray.init()

# Define a Ray task to find the longest word of each line
@ray.remote
def find_longest_word(line):
    # Using regular expressions to extract words
    words = re.findall(r'\b\w+\b', line)
    # Find the longest word and return
    if words:
        return max(words, key=len)
    return ""

# Read the file and process each line
def process_file(filepath):
    longest_words = []
    with open(filepath, 'r', encoding='utf-8') as file:
        # Submission of tasks and collection of results
        future_results = [find_longest_word.remote(line) for line in file]
        results = ray.get(future_results)
        # Find the longest word from all the returned longest words
        longest_word = max(results, key=len)
        return longest_word

# Specify your file path
file_path = 'D:\Задание 3\wiki.txt'
# Process the file and print the longest word
longest_word = process_file(file_path)
print("The longest word is：", longest_word)




