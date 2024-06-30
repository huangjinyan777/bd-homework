import ray
import re

# initialization Ray
ray.init()

@ray.remote
def count_words(line):
    # Use regular expressions to remove the tags, leaving only the body part
    text = re.split(r' <tabs> ', line)[-1]
    # Match all words using regular expressions
    words = re.findall(r'\b[A-Z][a-z]*\b', text)
    return words

def main():
    # Replace with your file path
    file_path = 'D:\Задание 3\wiki.txt'
    # Read file
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    # Distributing tasks using Ray
    futures = [count_words.remote(line) for line in lines]
    results = ray.get(futures)

    # Flat results list
    all_words = [word for sublist in results for word in sublist]

    # Counting word occurrences
    word_count = {}
    for word in all_words:
        if word in word_count:
            word_count[word] += 1
        else:
            word_count[word] = 1

    # Filter words with at least 10 occurrences
    frequent_words = {word: count for word, count in word_count.items() if count >= 10}

    # output result
    for word, count in frequent_words.items():
        print(f"{word}: {count}")

if __name__ == '__main__':
    main()
