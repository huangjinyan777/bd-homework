import mmap
import os
import struct
import threading
import time  # Importing time module

def thread_function(filename, offset, size, result, index):
    with open(filename, 'rb') as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        total_sum = 0
        min_val = float('inf')
        max_val = -float('inf')

        for i in range(offset, offset + size, 4):
            integer = struct.unpack('>I', mm[i:i + 4])[0]
            total_sum += integer
            min_val = min(min_val, integer)
            max_val = max(max_val, integer)

        result[index] = (total_sum, min_val, max_val)
        mm.close()

def process_file_multithreaded(filename, num_threads):
    file_size = os.path.getsize(filename)
    chunk_size = file_size // num_threads
    threads = []
    results = [None] * num_threads

    for i in range(num_threads):
        offset = i * chunk_size
        size = chunk_size if i < num_threads - 1 else file_size - offset
        thread = threading.Thread(target=thread_function, args=(filename, offset, size, results, i))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # Combine results
    total_sum = sum(x[0] for x in results)
    min_val = min(x[1] for x in results)
    max_val = max(x[2] for x in results)

    return total_sum, min_val, max_val

# Measure the execution time of process_file_multithreaded
start_time = time.time()
total_sum, min_val, max_val = process_file_multithreaded('random_integers.bin', 4)
end_time = time.time()
execution_time = end_time - start_time

# Print the results
print(f"Total Sum: {total_sum}, Min Value: {min_val}, Max Value: {max_val}")
print(f"Multithreaded Processing Time: {execution_time} seconds")
