###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    uniformity.py
# Author:  Stian Lysne slysne.dev@gmail.com
# 
# Copyright © 2025 Rakuten, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 
###############################################################################

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
