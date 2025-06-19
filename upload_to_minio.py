from minio import Minio
from minio.error import S3Error
import os

# --- MinIO 连接配置 ---
# 这是我们Docker里的MinIO服务的地址和凭证
MINIO_API_HOST = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# --- 要上传的文件和目标信息 ---
BUCKET_NAME = "large-data"  # 我们将在MinIO里创建的“文件夹”/桶
SOURCE_FILE_PATH = "sales_data_large.csv" # 你的原始数据文件，请确保它和本脚本在同一个目录下
DESTINATION_FILE_NAME = "sales/sales_data_large_raw.csv" # 上传后在MinIO里的路径和名字

def main():
    # 检查源文件是否存在
    if not os.path.exists(SOURCE_FILE_PATH):
        print(f"错误: 源文件 '{SOURCE_FILE_PATH}' 未找到。请确保该文件与脚本在同一目录下。")
        return

    # 1. 初始化 MinIO client.
    print("正在初始化MinIO客户端...")
    try:
        client = Minio(
            MINIO_API_HOST,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False  # 因为我们是本地http连接，所以设置为False
        )
    except Exception as e:
        print(f"初始化MinIO客户端失败: {e}")
        return

    # 2. 确保 bucket 存在，如果不存在就创建它
    try:
        print(f"正在检查Bucket '{BUCKET_NAME}' 是否存在...")
        found = client.bucket_exists(BUCKET_NAME)
        if not found:
            client.make_bucket(BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' 已创建。")
        else:
            print(f"Bucket '{BUCKET_NAME}' 已存在。")

        # 3. 上传文件
        print(f"正在上传 '{SOURCE_FILE_PATH}' 到 Bucket '{BUCKET_NAME}' 中的 '{DESTINATION_FILE_NAME}'...")
        client.fput_object(
            BUCKET_NAME, DESTINATION_FILE_NAME, SOURCE_FILE_PATH,
        )
        print("✅ 文件上传成功!")
        print(f"请访问 http://localhost:9001 并使用 minioadmin/minioadmin 登录来验证。")

    except S3Error as exc:
        print(f"上传过程中发生错误: {exc}")

if __name__ == "__main__":
    main()