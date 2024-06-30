import numpy as np

# Generate 50000 32-bit random integers
random_integers = np.random.randint(-2**31, 2**31, size=50000, dtype=np.int32)

# Write these integers to the file, each number on a separate line
with open('D:\\random_integers.txt', 'w') as file:
    for number in random_integers:
        file.write(f"{number}\n")
