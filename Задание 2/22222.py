import dask
import dask.bag as db
from sympy import factorint
import time

def process_line(line):
    number = int(line.strip())
    factors = factorint(number)
    return sum(factors.values())

def count_prime_factors_dask(filename):
    with open(filename, 'r') as file:
        lines = file.readlines()

    bag = db.from_sequence(lines, npartitions=4)
    result = bag.map(process_line).compute()
    return sum(result)

if __name__ == "__main__":
    filename = 'D:\\random_integers.txt'
    start_time = time.time()
    total_count = count_prime_factors_dask(filename)
    end_time = time.time()
    print(f"Total number of prime multipliers: {total_count}")
    print(f"execution time: {end_time - start_time} second (of time)")




