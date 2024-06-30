import os
import struct
import random

def create_binary_file(filename, num_integers):
    print("Starting to create a file...")
    with open(filename, 'wb') as file:
        for i in range(num_integers):
            if i % (1024 * 1024) == 0:  # Prints progress once for every 4MB of data written.
                print(f"written {i // 1024 // 1024} MB data...")
            integer = random.randint(0, 2**32 - 1)
            file.write(struct.pack('>I', integer))
    print("File creation is complete.")

# Create a file with a size of 2GB
# 4 bytes per 32-bit integer, number of integers to generate
num_integers = 2 * 1024 * 1024 * 1024 // 4
create_binary_file('random_integers.bin', num_integers)

