import struct  # Importing struct modules


def process_file_sequential(filename):
    total_sum = 0
    min_val = float('inf')
    max_val = -float('inf')

    with open(filename, 'rb') as file:
        while True:
            bytes_read = file.read(4)
            if not bytes_read:
                break
            integer = struct.unpack('>I', bytes_read)[0]
            total_sum += integer
            min_val = min(min_val, integer)
            max_val = max(max_val, integer)

    return total_sum, min_val, max_val


# Read and process files
total_sum, min_val, max_val = process_file_sequential('random_integers.bin')
print(f"Total Sum: {total_sum}, Min Value: {min_val}, Max Value: {max_val}")

