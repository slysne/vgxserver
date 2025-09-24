# pip install matplotlib numpy

from pyvgx import strhash128
import matplotlib.pyplot as plt

bit_counts = [0]*128
num_samples = 100000

for i in range(num_samples):
    h = int(strhash128(str(i)), 16)
    for bit in range(128):
        if h & (1 << bit):
            bit_counts[bit] += 1

plt.bar(range(128), [b / num_samples for b in bit_counts])
plt.xlabel("Output Bit Index")
plt.ylabel("Proportion of 1s")
plt.title("Bit Distribution Across Hash Output")
plt.show()

