# 个人版数据湖与BI分析平台 (V2.0)

## 项目简介

本项目旨在模拟并实践一套现代化的、从数据采集到可视化分析的全链路数据工程解决方案。平台整合了数据湖、ETL处理、数据仓库和BI可视化等关键技术，能够处理百万级数据量，并最终以可交互的数据仪表盘形式呈现业务洞察。

## 项目亮点

- **全链路实践：** 涵盖了数据工程师工作的完整流程，从原始数据入湖到最终的BI展现。
- **技术栈先进：** 采用了Spark、MinIO、PostgreSQL、Superset、Docker等业界主流的开源技术。
- **性能可扩展：** 使用Spark作为分布式计算引擎，并利用物化视图等数据库优化手段，确保了平台在百万级数据量下的高性能表现。
- **数据建模实践：** 在数据仓库层面，应用了经典的星型模型（事实表+维度表），体现了对数据仓库理论的理解和实践。
- **工程化部署：** 整个平台通过Docker和Docker Compose进行容器化部署和管理，便于移植和扩展。

## 技术架构

*在此处可以放一张您绘制的架构图，会非常加分！*

- **数据湖 (Data Lake):** **MinIO** - 用于存储原始的CSV销售数据。
- **ETL引擎 (ETL Engine):** **Apache Spark (PySpark)** - 负责从MinIO读取数据，进行清洗、转换、特征工程（如计算`total_price`），并构建星型模型。
- **数据仓库 (Data Warehouse):** **PostgreSQL** - 存储经过Spark处理后的、干净规整的事实表（`fact_sales`）和维度表（`dim_products`）。
- **性能优化层:** **PostgreSQL Materialized View** - 创建物化视图 `mv_sales_analytics`，预计算JOIN结果，大幅提升BI查询性能。
- **BI可视化 (BI Tool):** **Apache Superset** - 连接PostgreSQL，制作可交互的数据仪表盘。
- **容器化与编排:** **Docker & Docker Compose** - 管理和运行整个多组件平台。

## 项目演进之路 (V1.0 -> V2.0)

项目初期，为了快速验证流程，我搭建了一个V1.0的轻量级原型（**Pandas + SQLite**）。该原型成功实现了基础的BI分析，但也暴露了性能和扩展性的问题。

为了解决这些问题，我将架构全面升级至V2.0。在升级过程中，我解决了诸多挑战，包括但不限于：
- **Docker多服务网络通信与配置问题。**
- **Windows环境下Spark与Java、Hadoop的复杂环境配置与依赖版本冲突。**
- **百万级数据量下BI查询的超时问题，并通过创建索引和物化视图成功进行了性能优化。**

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

*在这里可以放入几张您最满意的仪表盘截图。*

---