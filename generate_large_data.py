import csv
import random
from datetime import datetime, timedelta

# --- 配置项 ---
NUM_ROWS = 1000000  # 我们要生成一百万行数据！
OUTPUT_FILE = 'sales_data_large.csv'

# --- 基础数据 ---
PRODUCTS = [
    {'id': 'P001', 'name': 'Laptop', 'category': 'Electronics', 'price_range': (7000, 9000)},
    {'id': 'P002', 'name': 'Mouse', 'category': 'Electronics', 'price_range': (100, 200)},
    {'id': 'P003', 'name': 'Keyboard', 'category': 'Electronics', 'price_range': (250, 400)},
    {'id': 'P004', 'name': 'T-shirt', 'category': 'Clothing', 'price_range': (80, 150)},
    {'id': 'P005', 'name': 'Coffee Maker', 'category': 'Appliances', 'price_range': (400, 600)},
    {'id': 'P006', 'name': 'Book - Python', 'category': 'Books', 'price_range': (70, 100)},
    {'id': 'P007', 'name': 'Desk Lamp', 'category': 'Appliances', 'price_range': (150, 250)},
    {'id': 'P008', 'name': 'Sneakers', 'category': 'Clothing', 'price_range': (300, 800)},
    {'id': 'P009', 'name': 'Monitor', 'category': 'Electronics', 'price_range': (1500, 3000)},
    {'id': 'P010', 'name': 'Backpack', 'category': 'Accessories', 'price_range': (200, 500)},
]
CITIES = ['Beijing', 'Shanghai', 'Guangzhou', 'Shenzhen', 'Chengdu', 'Hangzhou', 'Wuhan', 'Chongqing']
START_DATE = datetime(2022, 1, 1)
END_DATE = datetime(2024, 6, 1)

def generate_random_date(start, end):
    """生成一个在指定范围内的随机日期"""
    delta = end - start
    random_days = random.randrange(delta.days)
    return start + timedelta(days=random_days)

def main():
    print(f"准备生成 {NUM_ROWS:,} 行模拟销售数据...")
    
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # 写入表头
        writer.writerow(['OrderID', 'UserID', 'ProductID', 'ProductName', 'Category', 'Price', 'Quantity', 'OrderDate', 'City'])
        
        for i in range(1, NUM_ROWS + 1):
            product = random.choice(PRODUCTS)
            city = random.choice(CITIES)
            order_date = generate_random_date(START_DATE, END_DATE).strftime('%Y-%m-%d')
            
            row = [
                i,  # OrderID
                random.randint(100, 50000),  # UserID
                product['id'],  # ProductID
                product['name'],  # ProductName
                product['category'],  # Category
                random.randint(product['price_range'][0], product['price_range'][1]),  # Price
                random.randint(1, 5),  # Quantity
                order_date,  # OrderDate
                city,  # City
            ]
            writer.writerow(row)
            
            if i % 100000 == 0:
                print(f"已生成 {i:,} / {NUM_ROWS:,} 行...")

    print(f"🎉 成功生成文件 '{OUTPUT_FILE}'，包含 {NUM_ROWS:,} 行数据！")


if __name__ == "__main__":
    main()