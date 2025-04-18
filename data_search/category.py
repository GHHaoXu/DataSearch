import dask.dataframe as dd
import json
import pandas as pd

# 设置大文件夹路径
input_folder = '/home/xh/data_search/cleaned_parts/*/*.parquet'  # 根据实际路径调整
print('开始读取数据...')
# 使用Dask读取所有parquet文件
df = dd.read_parquet(input_folder)

# 解析purchase_history字段并提取种类信息
def extract_category(purchase_history):
    try:
        # 将purchase_history字段转换为字典
        purchase_data = json.loads(purchase_history)
        return purchase_data.get("category", "")
    except (ValueError, TypeError):
        return ""  # 如果无法解析，返回空字符串
print('开始提取字段...')
# 使用map操作解析purchase_history字段，提取购买种类
df['purchase_category'] = df['purchase_history'].map(extract_category, meta=('purchase_history', 'object'))
print('开始计算种类...')
# 获取所有唯一的购买种类
unique_categories = df['purchase_category'].unique().compute()
print('开始保存结果...')
# 将结果保存到CSV文件
output_file = '/home/xh/data_search/unique_purchase_categories.csv'  # 输出文件路径
categories_df = pd.DataFrame(unique_categories, columns=['purchase_category'])
categories_df.to_csv(output_file, index=False)

print(f"所有购买种类已保存到文件：{output_file}")
