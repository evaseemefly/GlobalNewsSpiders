import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

# ==================== Mac 中文字体修复 ====================
plt.rcParams['font.sans-serif'] = ['PingFang SC', 'Arial Unicode MS', 'Heiti TC', 'STHeiti']
plt.rcParams['axes.unicode_minus'] = False

def sigmoid(score, m=62, k=0.12):
    """S 型仓位曲线"""
    return 1 / (1 + np.exp(-k * (score - m)))

# 生成数据
scores = np.linspace(0, 100, 500)
positions = sigmoid(scores) * 100

# 绘图
plt.figure(figsize=(11, 7))
plt.plot(scores, positions, color='#e74c3c', linewidth=3.5, label='S型仓位系数')

# 关键标注线
plt.axvline(62, color='gray', linestyle='--', alpha=0.8, label='中点 (62分 ≈ 50%仓位)')
plt.axhline(50, color='gray', linestyle='--', alpha=0.6)

# 标注关键得分点
key_points = [30, 43, 50, 60, 62, 70, 75, 80, 90]
for s in key_points:
    p = sigmoid(s) * 100
    plt.scatter(s, p, color='blue', zorder=5, s=50)
    plt.annotate(f'{s}分\n{p:.1f}%',
                 xy=(s, p), xytext=(3, 8),
                 textcoords='offset points', fontsize=10, ha='left')

plt.title('S型仓位曲线（基本面得分 → 建议仓位系数）', fontsize=18, fontweight='bold', pad=20)
plt.xlabel('基本面得分 (0~100)', fontsize=13)
plt.ylabel('建议仓位系数 (%)', fontsize=13)
plt.grid(True, alpha=0.3)
plt.legend(fontsize=12)
plt.tight_layout()

# 保存图片（推荐）
save_path = Path("sigmoid_position_curve.png")
plt.savefig(save_path, dpi=200, bbox_inches='tight')
print(f"✅ 图片已保存至: {save_path}")

plt.show()