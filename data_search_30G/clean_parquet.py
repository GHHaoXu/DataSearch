import dask.dataframe as dd
import pandas as pd
import os
import glob
import time

input_folder = '/home/xh/30G_data'
output_clean = 'cleaned_parts'
output_deleted = 'deleted_parts'
timing_log_path = 'timing_log.csv'
os.makedirs(output_clean, exist_ok=True)
os.makedirs(output_deleted, exist_ok=True)

province_city_map = {
    "北京市": ["北京"],
    "天津市": ["天津"],
    "上海市": ["上海"],
    "重庆市": ["重庆"],
    "河北省": ["石家庄", "唐山", "秦皇岛", "邯郸", "邢台", "保定", "张家口", "承德", "沧州", "廊坊", "衡水"],
    "山西省": ["太原", "大同", "阳泉", "长治", "晋城", "朔州", "晋中", "运城", "忻州", "临汾", "吕梁"],
    "辽宁省": ["沈阳", "大连", "鞍山", "抚顺", "本溪", "丹东", "锦州", "营口", "阜新", "辽阳", "盘锦", "铁岭", "朝阳", "葫芦岛"],
    "吉林省": ["长春", "吉林", "四平", "辽源", "通化", "白山", "松原", "白城", "延边朝鲜族自治州"],
    "黑龙江省": ["哈尔滨", "齐齐哈尔", "鸡西", "鹤岗", "双鸭山", "大庆", "伊春", "佳木斯", "七台河", "牡丹江", "黑河", "绥化", "大兴安岭"],
    "江苏省": ["南京", "无锡", "徐州", "常州", "苏州", "南通", "连云港", "淮安", "盐城", "扬州", "镇江", "泰州", "宿迁"],
    "浙江省": ["杭州", "宁波", "温州", "嘉兴", "湖州", "绍兴", "金华", "衢州", "舟山", "台州", "丽水"],
    "安徽省": ["合肥", "芜湖", "蚌埠", "淮南", "马鞍山", "淮北", "铜陵", "安庆", "黄山", "滁州", "阜阳", "宿州", "六安", "亳州", "池州", "宣城"],
    "福建省": ["福州", "厦门", "莆田", "三明", "泉州", "漳州", "南平", "龙岩", "宁德"],
    "江西省": ["南昌", "景德镇", "萍乡", "九江", "新余", "鹰潭", "赣州", "吉安", "宜春", "抚州", "上饶"],
    "山东省": ["济南", "青岛", "淄博", "枣庄", "东营", "烟台", "潍坊", "济宁", "泰安", "威海", "日照", "莱芜", "临沂", "德州", "聊城", "滨州", "菏泽"],
    "河南省": ["郑州", "开封", "洛阳", "平顶山", "安阳", "鹤壁", "新乡", "焦作", "濮阳", "许昌", "漯河", "三门峡", "南阳", "商丘", "信阳", "周口", "驻马店"],
    "湖北省": ["武汉", "黄石", "十堰", "宜昌", "襄阳", "鄂州", "荆门", "孝感", "荆州", "黄冈", "咸宁", "随州", "恩施土家族苗族自治州"],
    "湖南省": ["长沙", "株洲", "湘潭", "衡阳", "邵阳", "岳阳", "常德", "张家界", "益阳", "郴州", "永州", "怀化", "娄底", "湘西土家族苗族自治州"],
    "广东省": ["广州", "深圳", "珠海", "汕头", "韶关", "佛山", "江门", "湛江", "茂名", "肇庆", "惠州", "梅州", "汕尾", "河源", "阳江", "清远", "东莞", "中山", "潮州", "揭阳", "云浮"],
    "广西壮族自治区": ["南宁", "柳州", "桂林", "梧州", "北海", "防城港", "钦州", "贵港", "玉林", "百色", "贺州", "河池", "来宾", "崇左"],
    "海南省": ["海口", "三亚", "三沙", "儋州"],
    "四川省": ["成都", "自贡", "攀枝花", "泸州", "德阳", "绵阳", "广元", "遂宁", "内江", "乐山", "南充", "眉山", "宜宾", "广安", "达州", "雅安", "巴中", "资阳", "阿坝藏族羌族自治州", "甘孜藏族自治州", "凉山彝族自治州"],
    "贵州省": ["贵阳", "六盘水", "遵义", "安顺", "毕节", "铜仁", "黔西南布依族苗族自治州", "黔东南苗族侗族自治州", "黔南布依族苗族自治州"],
    "云南省": ["昆明", "曲靖", "玉溪", "保山", "昭通", "丽江", "普洱", "临沧", "楚雄彝族自治州", "红河哈尼族彝族自治州", "文山壮族苗族自治州", "西双版纳傣族自治州", "大理白族自治州", "德宏傣族景颇族自治州", "怒江傈僳族自治州", "迪庆藏族自治州"],
    "西藏自治区": ["拉萨", "日喀则", "昌都", "林芝", "山南", "那曲", "阿里"],
    "陕西省": ["西安", "铜川", "宝鸡", "咸阳", "渭南", "延安", "汉中", "榆林", "安康", "商洛"],
    "甘肃省": ["兰州", "嘉峪关", "金昌", "白银", "天水", "武威", "张掖", "平凉", "酒泉", "庆阳", "定西", "陇南", "临夏回族自治州", "甘南藏族自治州"],
    "青海省": ["西宁", "海东", "海北藏族自治州", "黄南藏族自治州", "海南藏族自治州", "果洛藏族自治州", "玉树藏族自治州", "海西蒙古族藏族自治州"],
    "宁夏回族自治区": ["银川", "石嘴山", "吴忠", "固原", "中卫"],
    "新疆维吾尔自治区": ["乌鲁木齐", "克拉玛依", "吐鲁番", "哈密", "昌吉回族自治州", "博尔塔拉蒙古自治州", "巴音郭楞蒙古自治州", "阿克苏地区", "克孜勒苏柯尔克孜自治州", "喀什地区", "和田地区", "伊犁哈萨克自治州", "塔城地区", "阿勒泰地区"],
    "香港特别行政区": ["香港"],
    "澳门特别行政区": ["澳门"],
    "台湾省": ["台北", "高雄", "台中", "台南", "新竹", "基隆", "嘉义", "彰化"]
}
province_pattern = '|'.join(province_city_map.keys())
city_pattern = '|'.join({c for cities in province_city_map.values() for c in cities})
valid_pairs = pd.DataFrame([
    {'province': p, 'city': c}
    for p, cities in province_city_map.items()
    for c in cities
])
valid_pairs_dd = dd.from_pandas(valid_pairs, npartitions=1)

file_list = sorted(glob.glob(os.path.join(input_folder, '*.parquet')))
print(f"共找到 {len(file_list)} 个 parquet 文件")

timing_log = []

def log_step(file, step_name, start_time):
    end_time = time.time()
    duration = round(end_time - start_time, 2)
    print(f" [{step_name}] 用时: {duration} 秒")
    timing_log.append({'file': file, 'step': step_name, 'duration_seconds': duration})
    return end_time

def to_hash_ddf(df):
    if not isinstance(df, dd.DataFrame):
        df = dd.from_pandas(df, npartitions=5)
    return df.assign(row_hash=df.astype(str).sum(axis=1).map(hash, meta=('row_hash', 'int64')))[['row_hash']]

total_original = 0
total_cleaned = 0
total_deleted = 0
total_missing = 0
total_income_age = 0
total_invalid_addr = 0

for file_path in file_list:
    basename = os.path.basename(file_path).replace('.parquet', '')
    print(f"\n 正在处理: {basename}.parquet")
    file_start = time.time()

    df = dd.read_parquet(file_path)
    original_count = df.shape[0].compute()
    total_original += original_count
    print(f" 原始数据量: {original_count:,}")

    t0 = time.time()

    # 缺失值
    key_cols = ['age', 'income', 'credit_score', 'chinese_address']
    missing_mask = df[key_cols].isnull().any(axis=1)
    missing_df = df[missing_mask].compute()
    num_missing = len(missing_df)
    print(f"   缺失值: {num_missing:,}（{num_missing / original_count:.2%}）")
    total_missing += num_missing
    t0 = log_step(basename, 'missing_check', t0)

    # 高龄高收入
    income_age_mask = (df['age'] > 80) & (df['income'] > 500000)
    income_age_df = df[income_age_mask].compute()
    num_income_age = len(income_age_df)
    print(f"   高龄高收入: {num_income_age:,}（{num_income_age / original_count:.2%}）")
    total_income_age += num_income_age
    t0 = log_step(basename, 'income_age_check', t0)

    # 地址解析与省市不匹配
    df = df.assign(
        province=df['chinese_address'].str.extract(f'({province_pattern})', expand=False),
        city=df['chinese_address'].str.extract(f'({city_pattern})', expand=False)
    )
    df = df.merge(valid_pairs_dd, on=['province', 'city'], how='left', indicator=True)
    df['province_city_mismatch'] = df['_merge'] == 'left_only'
    df = df.drop('_merge', axis=1)
    invalid_addr_df = df[df['province_city_mismatch']].compute()
    num_invalid_addr = len(invalid_addr_df)
    print(f"   省市不匹配: {num_invalid_addr:,}（{num_invalid_addr / original_count:.2%}）")
    total_invalid_addr += num_invalid_addr
    t0 = log_step(basename, 'addr_check', t0)

    # 安全合并异常 row_hash（逐批）
    print(" 生成 row_hash 并合并异常...")
    missing_hash = to_hash_ddf(missing_df)
    income_age_hash = to_hash_ddf(income_age_df)
    invalid_addr_hash = to_hash_ddf(invalid_addr_df)
    all_delete_hashes = dd.concat([missing_hash, income_age_hash, invalid_addr_hash]).drop_duplicates()
    df = df.assign(row_hash=df.astype(str).sum(axis=1).map(hash, meta=('row_hash', 'int64')))
    df = df.merge(all_delete_hashes, on='row_hash', how='left', indicator=True)
    clean_df = df[df['_merge'] == 'left_only'].drop(columns=['_merge', 'row_hash'])
    
    # 保存
    clean_df.to_parquet(f"{output_clean}/clean_{basename}.parquet", write_index=False)

    clean_count = clean_df.shape[0].compute()
    deleted_count = original_count - clean_count
    deleted_ratio = deleted_count / original_count
    total_cleaned += clean_count
    total_deleted += deleted_count

    print(f" 清洗完成：删除 {deleted_count:,} 条，占比 {deleted_ratio:.2%}")
    print(f" 保存至: {output_clean}/clean_{basename}.parquet")

    t0 = log_step(basename, 'clean_and_save', t0)

print("\\n  所有文件处理完毕")
print(f" 原始总量: {total_original:,}")
print(f" 删除异常: {total_deleted:,}（{total_deleted / total_original:.2%}）")
print(f" 清洗后: {total_cleaned:,}")
print(f" 明细：缺失 {total_missing:,}，高龄高收入 {total_income_age:,}，省市不匹配 {total_invalid_addr:,}")

pd.DataFrame(timing_log).to_csv(timing_log_path, index=False)
print(f" 各步骤耗时写入日志文件：{timing_log_path}")