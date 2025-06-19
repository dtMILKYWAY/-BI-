from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

# --- 配置信息 ---
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
POSTGRES_URL = "jdbc:postgresql://localhost:5432/superset"
POSTGRES_USER = "superset"
POSTGRES_PASSWORD = "superset"

def main():
    print("正在初始化SparkSession (使用PySpark 3.2.4)...")
    
    try:
        # 1. 初始化 SparkSession
        #    使用经过验证的、与PySpark 3.2.4最兼容的依赖组合
        spark = (
            SparkSession.builder.appName("SalesETL_v3")
            
            # --- 依赖包配置 (这是关键) ---
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026,org.postgresql:postgresql:42.5.0")
            # --- 核心修复：为Java 9+ 的模块化系统添加--add-opens参数 ---
            .config(
                "spark.driver.extraJavaOptions",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
                "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
            )
            
            .getOrCreate()
        )

        # 2. 获取 SparkContext 并设置 Hadoop 配置
        #    这是保证配置生效的最高优先级方式
        sc = spark.sparkContext
        sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", MINIO_ENDPOINT)
        sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", MINIO_ACCESS_KEY)
        sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_SECRET_KEY)
        sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        
        print("SparkSession 初始化成功!")
        
    except Exception as e:
        print(f"SparkSession 初始化失败: {e}")
        return

    try:
        # 3. 从MinIO读取原始数据
        print("正在从MinIO读取原始数据...")
        raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv("s3a://large-data/sales/sales_data_large_raw.csv")
        print("原始数据读取成功，数据预览：")
        raw_df.show(5)

        # ... (后续的数据处理代码保持不变) ...
        # 4. 数据转换与清洗
        print("正在转换数据...")
        transformed_df = raw_df.withColumn("TotalPrice", col("Price") * col("Quantity")) \
                               .withColumn("OrderDate", col("OrderDate").cast("date"))

        # 5. 创建维度表和事实表
        print("正在创建维度表和事实表...")
        dim_products = transformed_df.select("ProductID", "ProductName", "Category", "Price").distinct() \
                                     .withColumn("product_key", monotonically_increasing_id())
        print("产品维度表 (dim_products) 预览：")
        dim_products.show(5)

        fact_sales = transformed_df.join(dim_products, on="ProductID", how="left") \
                                   .select(
                                       col("OrderID").alias("order_id"),
                                       col("UserID").alias("user_id"),
                                       col("product_key"),
                                       col("OrderDate").alias("order_date"),
                                       col("City").alias("city"),
                                       col("Quantity").alias("quantity"),
                                       col("TotalPrice").alias("total_price")
                                   )
        print("销售事实表 (fact_sales) 预览：")
        fact_sales.show(5)

        # 6. 将处理好的表写入PostgreSQL
        print("正在将表写入PostgreSQL...")
        db_properties = {
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
        dim_products.write.jdbc(url=POSTGRES_URL, table="dim_products", mode="overwrite", properties=db_properties)
        print("✅ 维度表 'dim_products' 写入成功。")

        fact_sales.write.jdbc(url=POSTGRES_URL, table="fact_sales", mode="overwrite", properties=db_properties)
        print("✅ 事实表 'fact_sales' 写入成功。")

        print("🎉 ETL 流程全部完成!")

    except Exception as e:
        print(f"ETL处理过程中发生错误: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()
            print("SparkSession已停止。")

if __name__ == "__main__":
    main()