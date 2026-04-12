import os
from pyspark.sql import SparkSession, functions
from CustomTPCDS import CustomTPCDS
from delta import DeltaTable

# To be discussed
tpcds_zorder_map = {
    "store_sales": ["ss_sold_date_sk", "ss_item_sk"],
    "store_returns": ["sr_returned_date_sk", "sr_item_sk"],
    "catalog_sales": ["cs_sold_date_sk", "cs_item_sk"],
    "catalog_returns": ["cr_returned_date_sk", "cr_item_sk"],
    "web_sales": ["ws_sold_date_sk", "ws_item_sk"],
    "web_returns": ["wr_returned_date_sk", "wr_item_sk"],
    "inventory": ["inv_date_sk", "inv_item_sk"]
}

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
        data_format: str,
        num_runs: int,
        queries_repeat_times: int,
        queries: str,
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
        sleep_time=sleep_time
    )
    if data_format == 'iceberg':
        tpcds.map_tables_iceberg(namespace=namespace)
    else:
        tpcds.map_tables()
    return tpcds

def convert_parquet_to_delta(spark_session: SparkSession, source_path: str, destination_path: str, optimization_technique: str = ''):
    if os.path.exists(destination_path):
        return
    
    tables = [t for t in os.listdir(source_path)]

    for table in tables:
        parquet_table_path = os.path.join(source_path, table)
        delta_table_path = os.path.join(destination_path, table)
        df = spark_session.read.format('parquet').load(parquet_table_path)
        df.write.format('delta').mode('overwrite').save(delta_table_path)

        if (optimization_technique == 'zorder'):
            dt = DeltaTable.forPath(spark_session, delta_table_path)
            if table in tpcds_zorder_map:
                try:
                    z_cols = tpcds_zorder_map[table]
                    dt.optimize().executeZOrderBy(*z_cols)   
                except Exception as e:
                    print(f"Failed to optimize (Z-order) table {table} at {delta_table_path}: {e}")

        elif (optimization_technique == 'compaction'):
            dt = DeltaTable.forPath(spark_session, delta_table_path)
            if table in tpcds_zorder_map:
                try:
                    dt.optimize().executeCompaction()  
                except Exception as e:
                    print(f"Failed to optimize (compaction) table {table} at {delta_table_path}: {e}")

def convert_parquet_to_hudi(spark_session: SparkSession, source_path: str, destination_path: str, optimization_technique: str = ''):
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
        }

        index = optimization_technique.upper()

        # To be precised with Dr. Lorkiewicz
        match index:
            case 'BLOOM':
                hudi_options['hoodie.index.type'] = 'BLOOM'
            case 'GLOBAL_BLOOM':
                hudi_options['hoodie.index.type'] = 'GLOBAL_BLOOM'
            case 'SIMPLE':
                hudi_options['hoodie.index.type'] = 'SIMPLE'
            case 'GLOBAL_SIMPLE':
                hudi_options['hoodie.index.type'] = 'GLOBAL_SIMPLE'
            case 'HBASE':
                hudi_options['hoodie.index.type'] = 'HBASE'
                hudi_options['hoodie.index.hbase.zkquorum'] = 'localhost' 
            case 'INMEMORY':
                hudi_options['hoodie.index.type'] = 'INMEMORY'
            case 'BUCKET_SIMPLE':
                hudi_options['hoodie.index.type'] = 'BUCKET'
                hudi_options['hoodie.index.bucket.engine'] = 'SIMPLE'
                hudi_options['hoodie.bucket.index.num.buckets'] = '8' # values to be discussed
            # case 'BUCKET_CONSISTENT':
            #     hudi_options['hoodie.index.type'] = 'BUCKET'
            #     hudi_options['hoodie.index.bucket.engine'] = 'CONSISTENT_HASHING'
            #     hudi_options['hoodie.bucket.index.num.buckets'] = '8' # values to be discussed
            #     hudi_options['hoodie.bucket.index.min.num.buckets'] = '4' # values to be discussed
            #     hudi_options['hoodie.bucket.index.max.num.buckets'] = '12' # values to be discussed
            case 'RECORD_LEVEL_INDEX':
                hudi_options['hoodie.index.type'] = 'RECORD_LEVEL_INDEX'
                hudi_options['hoodie.metadata.record.index.enable'] = 'true'
            case 'GLOBAL_RECORD_LEVEL_INDEX':
                hudi_options['hoodie.index.type'] = 'GLOBAL_RECORD_LEVEL_INDEX'
                hudi_options['hoodie.metadata.record.index.enable'] = 'true'
            case _:
                ...

        df.write.format('hudi').options(**hudi_options).mode('overwrite').save(hudi_table_path)

def convert_parquet_to_iceberg(spark_session: SparkSession, source_path: str, namespace: str, optimization_technique: str = ''):
    
    tables = [t for t in os.listdir(source_path)]
    spark_session.sql(f'CREATE NAMESPACE IF NOT EXISTS {namespace}')

    for table in tables:
        parquet_table_path = os.path.join(source_path, table)
        iceberg_table = f'{namespace}.{table}'
        df = spark_session.read.format('parquet').load(parquet_table_path)
        df.write.format('iceberg').mode('overwrite').saveAsTable(iceberg_table)

        if optimization_technique == 'zorder':
            if table in tpcds_zorder_map:
                try:
                    z_cols = ', '.join(tpcds_zorder_map[table])
                    spark_session.sql(
                        f"""CALL nessie.system.rewrite_data_files(
                            table => '{iceberg_table}', 
                            strategy => 'sort', 
                            sort_order => 'zorder({z_cols})'
                        )
                    """)
                except Exception as e:
                    print(f'Failed to optimize (Z-order) table {table} at {iceberg_table}: {e}')
        elif optimization_technique == 'bloom':
            if table in tpcds_zorder_map:
                try:
                    for col in tpcds_zorder_map[table]:
                        spark_session.sql(f"""
                            ALTER TABLE {iceberg_table} 
                            SET TBLPROPERTIES ('write.parquet.bloom-filter-enabled.column.{col}'='true')
                        """)
                    spark_session.sql(f"CALL nessie.system.rewrite_data_files(table => '{iceberg_table}')")
                except Exception as e:
                    print(f'Failed to optimize (Bloom filters) table {table} at {iceberg_table}: {e}')

def get_datasource(spark_session: SparkSession, format: str, source_path: str, optimization_technique: str = '') -> str:
    match format:
        case 'delta':
            if (optimization_technique == ''):
                destination_path = f'{source_path}_{format}'
            else:
                destination_path = f'{source_path}_{format}_{optimization_technique}'
            convert_parquet_to_delta(spark_session=spark_session, source_path=source_path, destination_path=destination_path, optimization_technique=optimization_technique)
            return destination_path
        case 'hudi':
            data_path = os.path.abspath(source_path)
            if (optimization_technique == ''):
                destination_path = f'{data_path}_{format}'
            else:
                destination_path = f'{data_path}_{format}_{optimization_technique}'
            convert_parquet_to_hudi(spark_session=spark_session, source_path=data_path, destination_path=destination_path, optimization_technique=optimization_technique)
            return destination_path
        case 'iceberg':
            if (optimization_technique == ''):
                namespace = f'{source_path}_{format}'
            else:
                namespace = f'{source_path}_{format}_{optimization_technique}'
            convert_parquet_to_iceberg(spark_session=spark_session, source_path=source_path, namespace=namespace)
            return namespace
        case _:
            print('Unsupported format. Proceeding with parquet\n')
            return source_path