# 个人版数据湖与BI分析平台 

## 项目简介

本项目旨在模拟并实践一套现代化的、从数据采集到可视化分析的全链路数据工程解决方案。平台整合了数据湖、ETL处理、数据仓库和BI可视化等关键技术，能够处理百万级数据量，并最终以可交互的数据仪表盘形式呈现业务洞察。

## 项目亮点

- **全链路实践：** 涵盖了数据工程师工作的完整流程，从原始数据入湖到最终的BI展现。
- **技术栈先进：** 采用了Spark、MinIO、PostgreSQL、Superset、Docker等业界主流的开源技术。
- **性能可扩展：** 使用Spark作为分布式计算引擎，并利用物化视图等数据库优化手段，确保了平台在百万级数据量下的高性能表现。
- **数据建模实践：** 在数据仓库层面，应用了经典的星型模型（事实表+维度表），体现了对数据仓库理论的理解和实践。
- **工程化部署：** 整个平台通过Docker和Docker Compose进行容器化部署和管理，便于移植和扩展。

## 技术架构

- **数据湖 (Data Lake):** **MinIO** - 用于存储原始的CSV销售数据。
- **ETL引擎 (ETL Engine):** **Apache Spark (PySpark)** - 负责从MinIO读取数据，进行清洗、转换、特征工程（如计算`total_price`），并构建星型模型。
- **数据仓库 (Data Warehouse):** **PostgreSQL** - 存储经过Spark处理后的、干净规整的事实表（`fact_sales`）和维度表（`dim_products`）。
- **性能优化层:** **PostgreSQL Materialized View** - 创建物化视图 `mv_sales_analytics`，预计算JOIN结果，大幅提升BI查询性能。
- **BI可视化 (BI Tool):** **Apache Superset** - 连接PostgreSQL，制作可交互的数据仪表盘。
- **容器化与编排:** **Docker & Docker Compose** - 管理和运行整个多组件平台。

## 项目演进之路

该原型成功实现了基础的BI分析，但也暴露了性能和扩展性的问题。

为了解决这些问题，我解决了诸多挑战，包括但不限于：
- **Docker多服务网络通信与配置问题。**

- ![3a18c67514dd4bdfdfd455eb3e842e32](https://github.com/user-attachments/assets/213c7356-1650-4793-a670-e5737806cec0)

- **Windows环境下Spark与Java、Hadoop的复杂环境配置与依赖版本冲突。**

- ![fae45a6c1911e62e1721cdcf7fd27b61](https://github.com/user-attachments/assets/e0984b38-82b2-4adc-9a26-44aea2956a0d)

- **百万级数据量下BI查询的超时问题，并通过创建索引和物化视图成功进行了性能优化。**
-- 1. 删除可能存在的旧视图
DROP MATERIALIZED VIEW IF EXISTS mv_sales_analytics;

-- 2. 创建新的物化视图
CREATE MATERIALIZED VIEW mv_sales_analytics AS
SELECT
  f.order_id,
  f.user_id,
  f.order_date,
  f.city,
  f.quantity,
  f.total_price,
  d."ProductName",
  d."Category",
  d."Price"
FROM public.fact_sales AS f
LEFT JOIN public.dim_products AS d ON f.product_key = d.product_key;

-- 3. 为新视图创建索引
CREATE INDEX idx_mv_sales_order_date ON mv_sales_analytics (order_date);

## 如何运行

1.  确保已安装 Docker, Docker Compose, Python, Java 17。
2.  配置好 `JAVA_HOME` 和 `HADOOP_HOME` 环境变量。
3.  在 `superset` 目录下执行 `docker-compose -f docker-compose-non-dev.yml up -d` 启动平台。
4.  等待服务启动后，在 `BIpy` 目录下依次执行：
    ```bash
    # (如果需要) 安装依赖
    pip install -r requirements.txt 
    # 生成百万级数据
    python generate_large_data.py
    # 数据入湖
    python upload_to_minio.py
    # 执行ETL
    python spark_etl.py
    ```
5.  访问 `http://localhost:8088` 查看Superset仪表盘。

## 最终成果展示
---
![078ded5e8783c7ff9340e80a28755018](https://github.com/user-attachments/assets/a2f4931c-df41-4f3f-a6f6-e488be8ef1c2)
![da6db7385ac2893dbaf1654b7e2be6af](https://github.com/user-attachments/assets/f558629b-bf5b-48cb-be99-adca9c5a96ec)
![cae7661b040304b2e5a55f3955d01a5e](https://github.com/user-attachments/assets/34512bcd-4a3f-48e7-9660-5ef27c910af3)
![938dd2681fe6d56e799a7bd2e8de6ee6](https://github.com/user-attachments/assets/411b3949-011b-4c1f-a30c-e61aaae29586)
![4a4a18997ef615ee7918ff7e354825ea](https://github.com/user-attachments/assets/a744e1e6-c86b-4c35-9a98-9b688608c038)
![8f705145181037c836435abb474ee949](https://github.com/user-attachments/assets/74063f8a-2e42-4110-8dc2-7882df6b3de6)



