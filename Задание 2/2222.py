import threading
from concurrent.futures import ThreadPoolExecutor
import time
from sympy import factorint


def count_prime_factors_threaded(filename):
    def process_lines(lines):
        local_count = 0
        for line in lines:
            number = int(line.strip())
            factors = factorint(number)
            local_count += sum(factors.values())
        with lock:
            nonlocal count
            count += local_count

    count = 0
    lock = threading.Lock()
    num_threads = 4
    chunk_size = 12500  # 50000 total integers divided by 4 threads

    with open(filename, 'r') as file:
        lines = file.readlines()

    # Split data into chunks for each thread
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

    start_time = time.time()
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(process_lines, chunks)
    end_time = time.time()

    return count, end_time - start_time


# The file path needs to be modified according to your environment
filename = 'D:\\random_integers.txt'

# Count prime factors using multithreading
threaded_count, threaded_time = count_prime_factors_threaded(filename)
print(f"Total number of prime multipliers: {threaded_count}")
print(f"execution time: {threaded_time} second (of time)")
