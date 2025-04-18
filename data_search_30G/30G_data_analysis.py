import dask.dataframe as dd
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import json
print('开始读数据...')
input_folder = '/home/xh/data_search_30G/cleaned_parts/*/*.parquet'
# 读取数据（使用dask读取parquet格式）
df = dd.read_parquet(input_folder)  # 你数据的路径
print('加载阈值...')
# 定义潜在高价值用户筛选标准
high_value_income_threshold = 100000  # 假设收入大于10万为潜在高价值用户
high_value_credit_score_threshold = 450  # 假设信用评分大于750为潜在高价值用户
high_value_age_range = (25, 45)  # 假设25岁到45岁的用户为潜在高价值用户
high_value_purchase_price_threshold = 500  # 假设平均购买价格大于500元为高价值购买
high_value_categories = ['家居', '电子产品','服装']  # 高价值商品的类别

# 解析purchase_history字段并提取平均价格和类别
def extract_average_price(purchase_history):
    try:
        # 将purchase_history字段转换为字典
        purchase_data = json.loads(purchase_history)
        return purchase_data.get("average_price", 0)
    except (ValueError, TypeError):
        return 0  # 如果无法解析，返回0

def extract_category(purchase_history):
    try:
        # 将purchase_history字段转换为字典
        purchase_data = json.loads(purchase_history)
        return purchase_data.get("category", "")
    except (ValueError, TypeError):
        return ""  # 如果无法解析，返回空字符串

# 使用map操作解析purchase_history字段，提取平均价格和类别
print('提取平均价格...')
df['average_purchase_price'] = df['purchase_history'].map(extract_average_price, meta=('purchase_history', 'float64'))
print('提取类别...')
df['purchase_category'] = df['purchase_history'].map(extract_category, meta=('purchase_history', 'object'))
# 筛选收入高于阈值的用户
print('筛选高收入用户...')
high_income_users = df[df['income'] > high_value_income_threshold]

# 筛选潜在高价值用户：收入高且购买平均价格和类别符合条件
print('筛选潜在高价值用户...')
high_value_users = high_income_users[
    (high_income_users['credit_score'] > high_value_credit_score_threshold) &
    (high_income_users['age'] >= high_value_age_range[0]) & 
    (high_income_users['age'] <= high_value_age_range[1]) &
    (high_income_users['average_purchase_price'] > high_value_purchase_price_threshold) &
    (high_income_users['purchase_category'].isin(high_value_categories))
]
print('开始转换为pandas...')
# 将dask dataframe转换为pandas dataframe以便进行可视化
high_value_users_pd = high_value_users.compute()

print('  收入与平均购买价格匹配分析...')
# 1. 收入与平均购买价格匹配分析
plt.figure(figsize=(10, 6))
sns.scatterplot(data=high_value_users_pd, x='income', y='average_purchase_price', hue='purchase_category', palette='viridis')
plt.title('Revenue vs. average purchase price matching analysis')
plt.xlabel('income')
plt.ylabel('Average purchase price')
plt.legend(title='category')
plt.tight_layout()
plt.savefig('/home/xh/data_search_30G/income_vs_purchase_price.png')  # 保存图片

# 2. 直方图（高价值用户的年龄分布）
print('  直方图（高价值用户的年龄分布）...')
plt.figure(figsize=(10, 6))
sns.histplot(high_value_users_pd['age'], bins=20, kde=True, color='blue')
plt.title('Histogram (age distribution of high-value users)')
plt.xlabel('age')
plt.ylabel('frequency')
plt.tight_layout()
plt.savefig('/home/xh/data_search_30G/high_value_users_age_distribution.png')  # 保存图片

# 3. 箱型图（收入、信用评分与购买价格分布）
print('  箱型图（收入分布）...')
plt.figure(figsize=(10, 6))
sns.boxplot(data=high_value_users_pd['income'])
plt.title('Box plot (distribution of income)')
plt.ylabel('value')
plt.tight_layout()
plt.savefig('/home/xh/data_search_30G/income_distribution.png')  # 保存图片

print('  箱型图（信用评分分布）...')
plt.figure(figsize=(10, 6))
sns.boxplot(data=high_value_users_pd['credit_score'])
plt.title('Box plot (distribution of credit score)')
plt.ylabel('value')
plt.tight_layout()
plt.savefig('/home/xh/data_search_30G/credit_distribution.png')  # 保存图片

print('  箱型图（购买价格分布）...')
plt.figure(figsize=(10, 6))
sns.boxplot(data=high_value_users_pd['average_purchase_price'])
plt.title('Box plot (distribution of purchase price)')
plt.ylabel('value')
plt.tight_layout()
plt.savefig('/home/xh/data_search_30G/purchase_price_distribution.png')  # 保存图片
print("可视化图像已保存到指定目录。")
