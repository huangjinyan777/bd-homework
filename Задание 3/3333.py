import ray
import re
from collections import Counter

# initialization Ray
ray.init()


# Define a Ray's remote function to count words
@ray.remote
def count_words(text):
    # Extract only words containing Latin letters
    words = re.findall(r'\b[a-zA-Z]+\b', text)
    # Count the number of occurrences of each word
    return Counter(words)


def main():
    file_path = 'D:\Задание 3\wiki.txt'  # Replace with your file path
    results = []

    # Reads the file line by line and creates a Ray task for each line
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            if '<tabs>' in line:
                parts = line.split('<tabs>')
                if len(parts) >= 3:
                    url, title, content = parts[0], parts[1], parts[2]
                    # Send title and content to remote function
                    results.append(count_words.remote(title + ' ' + content))

    # Merge all results
    total_counts = Counter()
    for result in results:
        total_counts.update(ray.get(result))

    # Find the most commonly used words
    most_common_word = total_counts.most_common(1)
    print("The most commonly used words are：", most_common_word)


if __name__ == "__main__":
    main()
