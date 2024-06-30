import ray
import re

# initialization Ray
ray.init()

# Define a Ray remote function that finds abbreviations in text
@ray.remote
def find_abbreviations(text):
    # Regular expressions match abbreviations like t.p., n.e.
    abbreviations = re.findall(r'\b\w+\.\w+\.', text)
    return abbreviations

def main():
    # Specify your file path
    file_path = 'D:\Задание 3\wiki.txt'

    # Create a list of all future objects.
    futures = []

    # Open and read files
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            # Assuming that the format of each line is: URL <tag> title <tag> full text
            parts = line.split('<tabs>')
            if len(parts) >= 3:
                content = parts[2]  # Access to the body of the text
                # Calls the remote function and adds the resulting object to the futures list
                future = find_abbreviations.remote(content)
                futures.append(future)

    # Collect all results
    results = ray.get(futures)

    # Output all found abbreviations
    all_abbreviations = set()
    for result in results:
        all_abbreviations.update(result)

    print(all_abbreviations)

if __name__ == '__main__':
    main()
