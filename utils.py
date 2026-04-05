import os
from pyspark.sql import SparkSession
from CustomTPCDS import CustomTPCDS

def create_spark_session(name: str) -> SparkSession:

    spark_session = SparkSession.builder \
        .appName(name) \
        .master('local[*]') \
        .config("spark.driver.memory", "16g") \
        .config("spark.jars.packages",
                ",".join([
                "io.delta:delta-spark_2.12:3.2.0",
                "ch.cern.sparkmeasure:spark-measure_2.12:0.27"
            ])) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.repositories", "https://maven-central.storage-download.googleapis.com/maven2/") \
        .getOrCreate()
    
    return spark_session

def load_data(
        spark_session: SparkSession,
        data_path: str,
        data_format: str = 'parquet',
        num_runs: int = 2,
        queries_repeat_times: int = 3,
        queries: list = [],
        queries_exclude: list = [],
        sleep_time: int = 1
    ) -> CustomTPCDS:

    tpcds = CustomTPCDS(
        spark_session=spark_session,
        data_path=data_path,
        data_format=data_format,
        num_runs=num_runs,
        queries_repeat_times=queries_repeat_times,
        queries=queries,
        queries_exclude=queries_exclude,
        sleep_time=sleep_time
    )

    tpcds.map_tables()
    return tpcds

def convert_parquet_to_delta(spark_session: SparkSession, source_path: str, destination_path: str):

    if os.path.exists(destination_path):
        return
    
    tables = [t for t in os.listdir(source_path)]

    for table in tables:
        parquet_table_path = os.path.join(source_path, table)
        delta_table_path = os.path.join(destination_path, table)
        df = spark_session.read.format('parquet').load(parquet_table_path)
        df.write.format('delta').mode('overwrite').save(delta_table_path)

def get_datasource(spark_session: SparkSession, format: str, source_path: str, destination_path: str) -> str:
    match format:
        case 'delta':
            convert_parquet_to_delta(spark_session=spark_session, source_path=source_path, destination_path=destination_path)
            return destination_path
        case _:
            print('Unsupported format. Proceeding with parquet\n')
            return source_path