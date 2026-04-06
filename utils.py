import os
from pyspark.sql import SparkSession, functions
from CustomTPCDS import CustomTPCDS

def create_spark_session(name: str, format: str, master: str, memory: int) -> SparkSession:

    match format:
        case 'delta':
            spark_session = SparkSession.builder \
                .appName(name) \
                .master(master) \
                .config("spark.driver.memory", f'{memory}g') \
                .config("spark.jars.packages",
                        ",".join([
                        "io.delta:delta-spark_2.12:3.2.0",
                        "ch.cern.sparkmeasure:spark-measure_2.12:0.27",
                    ])) \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.jars.repositories", "https://maven-central.storage-download.googleapis.com/maven2/") \
                .getOrCreate()
        case 'hudi':
            spark_session = SparkSession.builder \
                .appName(name) \
                .master(master) \
                .config("spark.driver.memory", f'{memory}g') \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.jars.packages",
                        ",".join([
                        "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0",
                        "ch.cern.sparkmeasure:spark-measure_2.12:0.27"
                    ])) \
                .config("spark.jars.repositories", "https://maven-central.storage-download.googleapis.com/maven2/") \
                .getOrCreate()
        case 'iceberg':
            nessie_url = "http://localhost:19120/api/v1"
            nessie_warehouse = f"file:///{os.path.abspath('spark-warehouse/iceberg')}"
            nessie_branch = "main"
            nessie_auth = "NONE"
            spark_session = SparkSession.builder \
                .appName(name) \
                .master(master) \
                .config("spark.driver.memory", f'{memory}g') \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.jars.packages",
                        ",".join([
                        "ch.cern.sparkmeasure:spark-measure_2.12:0.27",
                        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1",
                        "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.106.0"
                    ])) \
                .config("spark.sql.extensions", 
                        ",".join([
                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
                    ])) \
                .config("spark.sql.catalog.nessie.uri", nessie_url) \
                .config("spark.sql.catalog.nessie.ref", nessie_branch) \
                .config("spark.sql.catalog.nessie.authentication.type", nessie_auth) \
                .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
                .config("spark.sql.catalog.nessie.warehouse", nessie_warehouse) \
                .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.defaultCatalog", "nessie") \
                .config("spark.jars.repositories", "https://maven-central.storage-download.googleapis.com/maven2/") \
                .getOrCreate()
        case _:
            spark_session = SparkSession.builder \
                .appName(name) \
                .master(master) \
                .config("spark.driver.memory", f'{memory}g') \
                .config("spark.jars.packages", "ch.cern.sparkmeasure:spark-measure_2.12:0.27") \
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
        sleep_time: int = 1,
        namespace = '' # only for Iceberg
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
    if data_format == 'iceberg':
        tpcds.map_tables_iceberg(namespace=namespace)
    else:
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

def convert_parquet_to_hudi(spark_session: SparkSession, source_path: str, destination_path: str):
    if os.path.exists(destination_path):
        return
    
    tables = [t for t in os.listdir(source_path)]
    hudi_uri = f'file:///{os.path.abspath(destination_path)}'

    for table in tables:
        parquet_table_path = os.path.join(source_path, table)
        hudi_table_path = os.path.join(hudi_uri, table)
        df = spark_session.read.format('parquet').load(parquet_table_path)
        df = df.withColumn('hudi_pk', functions.expr('uuid()'))
        hudi_options = {
            'hoodie.table.name': table,
            'hoodie.datasource.write.recordkey.field': 'hudi_pk',
            'hoodie.datasource.write.precombine.field': 'hudi_pk',
            'hoodie.datasource.write.operation': 'bulk_insert',
            # 'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator',
            # 'hoodie.datasource.write.partitionpath.field': ''
        }
        df.write.format('hudi').options(**hudi_options).mode('overwrite').save(hudi_table_path)

def convert_parquet_to_iceberg(spark_session: SparkSession, source_path: str, namespace: str):
    tables = [t for t in os.listdir(source_path)]
    spark_session.sql(f'CREATE NAMESPACE IF NOT EXISTS {namespace}')

    for table in tables:
        parquet_table_path = os.path.join(source_path, table)
        iceberg_table = f'{namespace}.{table}'
        df = spark_session.read.format('parquet').load(parquet_table_path)
        df.write.format('iceberg').mode('overwrite').saveAsTable(iceberg_table)

def get_datasource(spark_session: SparkSession, format: str, source_path: str) -> str:
    match format:
        case 'delta':
            destination_path = f'{source_path}_{format}'
            convert_parquet_to_delta(spark_session=spark_session, source_path=source_path, destination_path=destination_path)
            return destination_path
        case 'hudi':
            data_path = os.path.abspath(source_path)
            destination_path = f'{data_path}_{format}'
            convert_parquet_to_hudi(spark_session=spark_session, source_path=data_path, destination_path=destination_path)
            return destination_path
        case 'iceberg':
            convert_parquet_to_iceberg(spark_session=spark_session, source_path=source_path, namespace=source_path)
            return source_path
        case _:
            print('Unsupported format. Proceeding with parquet\n')
            return source_path