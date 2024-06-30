import ray
import re

# initialization Ray
ray.init()


@ray.remote
def find_names(text):
    # Use regular expressions to find all words that begin with a capital letter
    names = re.findall(r'\b[A-Z][a-z]*\b', text)
    return names


def main():
    # Replace the following path with your file path
    file_path = 'D:\Задание 3\wiki.txt'
    names_count = {}

    with open(file_path, 'r', encoding='utf-8') as file:
        results = []
        for line in file:
            url, title, content = line.split('<tabs>')
            # Asynchronous processing of the body content of each line
            result = find_names.remote(content.strip())
            results.append(result)

        # Collection of results
        for result in results:
            names = ray.get(result)
            for name in names:
                if name in names_count:
                    names_count[name] += 1
                else:
                    names_count[name] = 1

    # The name of the output statistic and its number of occurrences
    for name, count in names_count.items():
        print(f"{name}: {count}")


if __name__ == '__main__':
    main()
