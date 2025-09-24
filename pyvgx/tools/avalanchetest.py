import random
from pyvgx import strhash128

def hex_to_bits(hexstr):
    return bin(int(hexstr, 16))[2:].zfill(128)

def hamming_distance(bits1, bits2):
    return sum(b1 != b2 for b1, b2 in zip(bits1, bits2))

def flip_bit(s, bit_index):
    b = bytearray(s)
    byte_idx = bit_index // 8
    bit_in_byte = bit_index % 8
    b[byte_idx] ^= 1 << bit_in_byte
    return bytes(b)

def run_avalanche_tests(num_tests=1000, input_size=8):
    total_distance = 0
    for _ in range(num_tests):
        original = bytes(random.getrandbits(8) for _ in range(input_size))
        flipped = flip_bit(original, random.randint(0, input_size * 8 - 1))
        
        h1 = hex_to_bits(strhash128(original.decode('latin1')))
        h2 = hex_to_bits(strhash128(flipped.decode('latin1')))
        
        dist = hamming_distance(h1, h2)
        total_distance += dist

    avg = total_distance / num_tests
    print(f"Average Hamming distance: {avg} bits out of 128 (~{(avg/128)*100:.2f}%)")

run_avalanche_tests( num_tests=1000, input_size=1 )
run_avalanche_tests( num_tests=10000, input_size=2 )
run_avalanche_tests( num_tests=10000, input_size=7 )
run_avalanche_tests( num_tests=10000, input_size=8 )
run_avalanche_tests( num_tests=100000, input_size=9 )
run_avalanche_tests( num_tests=100000, input_size=25 )
run_avalanche_tests( num_tests=100000, input_size=255 )
run_avalanche_tests( num_tests=100000, input_size=1255 )

