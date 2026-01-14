import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, Polygon
import numpy as np

# Claude/Anthropic color palette
COLORS = {
    'bg': '#FAF9F7',
    'card_bg': '#FFFFFF',
    'primary': '#D97757',
    'tan': '#C9A87C',
    'sage': '#7D8B74',
    'text': '#1F1F1F',
    'text_muted': '#6B6B6B',
    'border': '#E5E0DB',
    'success': '#6B8E5E',
    'error': '#C45B4A',
    'blue': '#5B7B9A',
    'purple': '#8B7BA5',
}

# Larger canvas, simpler layout
fig, ax = plt.subplots(1, 1, figsize=(24, 16))
fig.patch.set_facecolor(COLORS['bg'])
ax.set_facecolor(COLORS['bg'])
ax.set_xlim(0, 24)
ax.set_ylim(0, 16)
ax.set_aspect('equal')
ax.axis('off')

def draw_box(ax, x, y, w, h, text, color, subtext=None, fontsize=12):
    shadow = FancyBboxPatch((x+0.06, y-0.06), w, h,
                            boxstyle="round,pad=0.02,rounding_size=0.15",
                            facecolor='#00000010', edgecolor='none')
    ax.add_patch(shadow)

    box = FancyBboxPatch((x, y), w, h,
                         boxstyle="round,pad=0.02,rounding_size=0.15",
                         facecolor=color, edgecolor=color, linewidth=2)
    ax.add_patch(box)

    text_y = y + h/2 + (0.15 if subtext else 0)
    ax.text(x + w/2, text_y, text, ha='center', va='center', fontsize=fontsize,
            fontweight='bold', color='white', family='sans-serif')

    if subtext:
        ax.text(x + w/2, y + h/2 - 0.25, subtext, ha='center', va='center',
                fontsize=fontsize-3, color='#FFFFFFCC', family='sans-serif')

def draw_diamond(ax, x, y, w, h, text, color=COLORS['tan']):
    cx, cy = x + w/2, y + h/2
    pts = [(cx, cy+h/2), (cx+w/2, cy), (cx, cy-h/2), (cx-w/2, cy)]
    diamond = Polygon(pts, facecolor=color, edgecolor=color, linewidth=2)
    ax.add_patch(diamond)
    ax.text(cx, cy, text, ha='center', va='center', fontsize=11,
            fontweight='bold', color='white', family='sans-serif')

def draw_arrow(ax, start, end, color=COLORS['border'], lw=2.5):
    ax.annotate('', xy=end, xytext=start,
                arrowprops=dict(arrowstyle='->', color=color, lw=lw,
                               connectionstyle='arc3,rad=0'))

def draw_line(ax, points, color=COLORS['border'], lw=2.5):
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    ax.plot(xs, ys, color=color, linewidth=lw, solid_capstyle='round')

def draw_label(ax, x, y, text, color=COLORS['text_muted'], fontsize=10):
    ax.text(x, y, text, ha='center', va='center', fontsize=fontsize,
            fontweight='600', color=color, family='sans-serif')

# ============ TITLE ============
ax.text(12, 15.3, 'S3 Proxy — High-Level Flow', ha='center', va='center',
        fontsize=28, fontweight='bold', color=COLORS['text'], family='sans-serif')
ax.text(12, 14.7, 'Client-Side Encryption Proxy for AWS S3', ha='center', va='center',
        fontsize=14, color=COLORS['text_muted'], family='sans-serif')

# ============ MAIN FLOW ============

# 1. CLIENT REQUEST
draw_box(ax, 9.5, 13, 5, 1.1, 'Client Request', COLORS['blue'], 'S3 API call')

draw_arrow(ax, (12, 13), (12, 12.3))

# 2. PARSE & AUTH
draw_box(ax, 8.5, 10.8, 7, 1.3, 'Parse & Authenticate', COLORS['purple'], 'SigV4 signature verification')

# Auth failure branch
draw_line(ax, [(8.5, 11.45), (6.5, 11.45)])
draw_arrow(ax, (6.5, 11.45), (5.5, 11.45))
draw_box(ax, 2.5, 10.95, 3, 0.9, '403', COLORS['error'], 'Invalid')
draw_label(ax, 7, 11.8, 'FAIL', COLORS['error'], 9)

draw_arrow(ax, (12, 10.8), (12, 10.1))
draw_label(ax, 12.5, 10.45, 'PASS', COLORS['success'], 9)

# 3. ROUTING
draw_diamond(ax, 9.5, 8.5, 5, 1.4, 'Route Request', COLORS['tan'])

# Branch lines
draw_line(ax, [(9.5, 9.2), (4, 9.2), (4, 7.8)])  # Left branch
draw_line(ax, [(14.5, 9.2), (20, 9.2), (20, 7.8)])  # Right branch
draw_arrow(ax, (12, 8.5), (12, 7.8))  # Center branch

# 4. OPERATIONS (3 main paths)
draw_box(ax, 1.5, 6.3, 5, 1.3, 'PUT / POST', COLORS['primary'], 'Upload / Multipart')
draw_box(ax, 9.5, 6.3, 5, 1.3, 'GET', COLORS['blue'], 'Download')
draw_box(ax, 17.5, 6.3, 5, 1.3, 'LIST / HEAD / DELETE', COLORS['sage'], 'Metadata ops')

# Arrows down to encryption
draw_arrow(ax, (4, 6.3), (4, 5.6))
draw_arrow(ax, (12, 6.3), (12, 5.6))
draw_arrow(ax, (20, 6.3), (20, 5.6))

# 5. ENCRYPTION LAYER
# Background for encryption section
enc_bg = FancyBboxPatch((1, 3.8), 22, 1.6,
                        boxstyle="round,pad=0.02,rounding_size=0.2",
                        facecolor=COLORS['tan'], edgecolor='none', alpha=0.15)
ax.add_patch(enc_bg)

ax.text(12, 5.15, 'ENCRYPTION LAYER', ha='center', va='center',
        fontsize=12, fontweight='bold', color=COLORS['tan'], family='sans-serif',
        bbox=dict(boxstyle='round,pad=0.3', facecolor=COLORS['bg'], edgecolor='none'))

draw_box(ax, 1.5, 4, 5, 1, 'Encrypt', COLORS['tan'], 'AES-256-GCM')
draw_box(ax, 9.5, 4, 5, 1, 'Decrypt', COLORS['tan'], 'Unwrap DEK')
draw_box(ax, 17.5, 4, 5, 1, 'Pass-through', COLORS['border'], fontsize=11)
ax.text(20, 4.5, 'or read metadata', ha='center', va='center',
        fontsize=9, color=COLORS['text_muted'])

# Arrows down to S3
draw_arrow(ax, (4, 4), (4, 3.3))
draw_arrow(ax, (12, 4), (12, 3.3))
draw_arrow(ax, (20, 4), (20, 3.3))

# Converge to S3
draw_line(ax, [(4, 3.1), (4, 2.8), (20, 2.8), (20, 3.1)])
draw_line(ax, [(12, 3.1), (12, 2.8)])

# 6. S3 BACKEND
draw_box(ax, 8.5, 1.5, 7, 1.2, 'AWS S3', COLORS['primary'], 'Actual storage')
draw_arrow(ax, (12, 2.8), (12, 2.7))

# 7. RESPONSE
draw_arrow(ax, (12, 1.5), (12, 0.9))
draw_box(ax, 9.5, 0.1, 5, 0.7, 'Response', COLORS['success'], fontsize=11)

# ============ SIDE INFO BOXES ============

# Left side - Key info
info_bg = FancyBboxPatch((0.3, 0.3), 4.2, 2.8,
                         boxstyle="round,pad=0.05,rounding_size=0.15",
                         facecolor=COLORS['card_bg'], edgecolor=COLORS['border'], linewidth=1.5)
ax.add_patch(info_bg)

ax.text(2.4, 2.85, 'Encryption', ha='center', fontsize=11, fontweight='bold',
        color=COLORS['tan'], family='sans-serif')
ax.text(0.5, 2.4, '• AES-256-GCM', fontsize=9, color=COLORS['text_muted'])
ax.text(0.5, 2.0, '• Per-object DEK', fontsize=9, color=COLORS['text_muted'])
ax.text(0.5, 1.6, '• KEK wraps DEK', fontsize=9, color=COLORS['text_muted'])
ax.text(0.5, 1.2, '• 12-byte nonce', fontsize=9, color=COLORS['text_muted'])
ax.text(0.5, 0.8, '• 16-byte auth tag', fontsize=9, color=COLORS['text_muted'])

# Right side - Features
feat_bg = FancyBboxPatch((19.5, 0.3), 4.2, 2.8,
                         boxstyle="round,pad=0.05,rounding_size=0.15",
                         facecolor=COLORS['card_bg'], edgecolor=COLORS['border'], linewidth=1.5)
ax.add_patch(feat_bg)

ax.text(21.6, 2.85, 'Features', ha='center', fontsize=11, fontweight='bold',
        color=COLORS['purple'], family='sans-serif')
ax.text(19.7, 2.4, '• AWS SigV4 Auth', fontsize=9, color=COLORS['text_muted'])
ax.text(19.7, 2.0, '• Streaming (64KB)', fontsize=9, color=COLORS['text_muted'])
ax.text(19.7, 1.6, '• Multipart uploads', fontsize=9, color=COLORS['text_muted'])
ax.text(19.7, 1.2, '• Range requests', fontsize=9, color=COLORS['text_muted'])
ax.text(19.7, 0.8, '• Transparent proxy', fontsize=9, color=COLORS['text_muted'])

# ============ LEGEND ============
legend_items = [
    (COLORS['blue'], 'Client/Download'),
    (COLORS['primary'], 'Upload/Core'),
    (COLORS['sage'], 'Metadata'),
    (COLORS['tan'], 'Encryption'),
    (COLORS['purple'], 'Auth/Routing'),
]

legend_x = 0.5
for i, (color, label) in enumerate(legend_items):
    x_pos = legend_x + i * 4.7
    rect = FancyBboxPatch((x_pos, 14.6), 0.4, 0.35,
                          boxstyle="round,pad=0.02,rounding_size=0.08",
                          facecolor=color, edgecolor='none')
    ax.add_patch(rect)
    ax.text(x_pos + 0.55, 14.77, label, fontsize=9, va='center',
            color=COLORS['text_muted'], family='sans-serif')

plt.tight_layout()
plt.savefig('/Users/hgu/Desktop/sseproxy-python/s3proxy_flowchart.png', dpi=150,
            bbox_inches='tight', facecolor=COLORS['bg'], edgecolor='none')
plt.close()

print("High-level flowchart saved to: s3proxy_flowchart.png")
