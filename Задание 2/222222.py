import time
from multiprocessing import Pool
from sympy import factorint

def process_lines(lines):
    local_count = 0
    for line in lines:
        number = int(line.strip())
        factors = factorint(number)
        local_count += sum(factors.values())
    return local_count

def count_prime_factors_multiprocessing_external(filename):
    num_processes = 4
    chunk_size = 12500  # 50000 total integers divided by 4 processes

    with open(filename, 'r') as file:
        lines = file.readlines()

    # Split data into chunks for each process
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

    start_time = time.time()
    with Pool(num_processes) as pool:
        results = pool.map(process_lines, chunks)
    total_count = sum(results)
    end_time = time.time()

    return total_count, end_time - start_time

if __name__ == '__main__':
    # Use local file paths
    filename = 'D:\\random_integers.txt'

    # Count prime factors using multiprocessing with external function
    multiprocessing_count, multiprocessing_time = count_prime_factors_multiprocessing_external(filename)
    print(f"Total number of prime multipliers: {multiprocessing_count}")
    print(f"execution time: {multiprocessing_time} second (of time)")



