from sympy import factorint
import time

def count_prime_factors(filename):
    count = 0
    with open(filename, 'r') as file:
        for line in file:
            number = int(line.strip())
            factors = factorint(number)
            count += sum(factors.values())
    return count

# Setting the correct file path
filename = 'D:\\random_integers.txt'

# Record start time
start_time = time.time()

# Calculate the total number of prime multipliers after factorization of all numbers
prime_factors_count = count_prime_factors(filename)

# Record end time
end_time = time.time()

# Calculation of execution time
execution_time = end_time - start_time

print(f"Total number of prime multipliers: {prime_factors_count}")
print(f"execution time: {execution_time} second (of time)")

