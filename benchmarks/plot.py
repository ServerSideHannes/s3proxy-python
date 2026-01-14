#!/usr/bin/env python3
"""Generate benchmark comparison chart for README."""

import matplotlib.pyplot as plt
import numpy as np

# Benchmark results (from latest run)
data = {
    "Throughput\n(req/s)": (492.3, 199.6),
    "PUT Latency\n(ms)": (14.9, 29.1),
    "GET Latency\n(ms)": (5.6, 21.2),
}

fig, ax = plt.subplots(figsize=(8, 3.5))

x = np.arange(len(data))
width = 0.35

baseline = [v[0] for v in data.values()]
proxy = [v[1] for v in data.values()]

bars1 = ax.bar(x - width/2, baseline, width, label='Direct (MinIO)', color='#4CAF50', alpha=0.85)
bars2 = ax.bar(x + width/2, proxy, width, label='S3Proxy', color='#2196F3', alpha=0.85)

ax.set_ylabel('Value')
ax.set_xticks(x)
ax.set_xticklabels(data.keys())
ax.legend(loc='upper right')
ax.set_title('S3Proxy Performance (64KB objects, 10 concurrent)', fontsize=11, fontweight='bold')

# Add value labels on bars
for bar in bars1:
    height = bar.get_height()
    ax.annotate(f'{height:.0f}' if height > 10 else f'{height:.1f}',
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3), textcoords="offset points",
                ha='center', va='bottom', fontsize=9)

for bar in bars2:
    height = bar.get_height()
    ax.annotate(f'{height:.0f}' if height > 10 else f'{height:.1f}',
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3), textcoords="offset points",
                ha='center', va='bottom', fontsize=9)

# Add overhead annotation
ax.annotate('~60% overhead\n(extra network hop + encryption)',
            xy=(0, 350), fontsize=9, color='#666', style='italic')

plt.tight_layout()
plt.savefig('results/benchmark.png', dpi=150, bbox_inches='tight',
            facecolor='white', edgecolor='none')
plt.savefig('results/benchmark.svg', bbox_inches='tight',
            facecolor='white', edgecolor='none')
print("Saved: results/benchmark.png and results/benchmark.svg")
