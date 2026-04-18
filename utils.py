import os
import shutil
from pyspark.sql import SparkSession
from CustomTPCDS import CustomTPCDS
from delta import DeltaTable

tpcds_zorder_map = {
    "store_sales": ["ss_sold_date_sk", "ss_item_sk"],
    "store_returns": ["sr_returned_date_sk", "sr_item_sk"],
    "catalog_sales": ["cs_sold_date_sk", "cs_item_sk"],
    "catalog_returns": ["cr_returned_date_sk", "cr_item_sk"],
    "web_sales": ["ws_sold_date_sk", "ws_item_sk"],
    "web_returns": ["wr_returned_date_sk", "wr_item_sk"],
    "inventory": ["inv_date_sk", "inv_item_sk"],
    "store": ["s_store_sk"],
    "call_center": ["cc_call_center_sk"],
    "catalog_page": ["cp_catalog_page_sk"],
    "web_site": ["web_site_sk"],
    "web_page": ["wp_web_page_sk"],
    "warehouse": ["w_warehouse_sk"],
    "customer": ["c_customer_sk"],
    "date_dim": ["d_year", "d_month_seq"],
    "item": ["i_item_sk"]
}

tpcds_partition_map = {
    "store_sales": ["ss_store_sk"],
    "catalog_sales": ["cs_warehouse_sk"],
    "web_sales": ["ws_web_site_sk"],
    "inventory": ["inv_warehouse_sk"],
    "store_returns": ["sr_store_sk"],
    "catalog_returns": ["cr_warehouse_sk"],
    "web_returns": ["wr_web_page_sk"],
}

tpcds_pk = {
    "store_sales": ["ss_ticket_number", "ss_item_sk"],
    "store_returns": ["sr_ticket_number", "sr_item_sk"],
    "catalog_sales": ["cs_order_number", "cs_item_sk"],
    "catalog_returns": ["cr_order_number", "cr_item_sk"],
    "web_sales": ["ws_order_number", "ws_item_sk"],
    "web_returns": ["wr_order_number", "wr_item_sk"],
    "inventory": ["inv_date_sk", "inv_item_sk", "inv_warehouse_sk"],
    "store": ["s_store_sk"],
    "call_center": ["cc_call_center_sk"],
    "catalog_page": ["cp_catalog_page_sk"],
    "web_site": ["web_site_sk"],
    "web_page": ["wp_web_page_sk"],
    "warehouse": ["w_warehouse_sk"],
    "customer": ["c_customer_sk"],
    "customer_address": ["ca_address_sk"],
    "customer_demographics": ["cd_demo_sk"],
    "date_dim": ["d_date_sk"],
    "household_demographics": ["hd_demo_sk"],
    "item": ["i_item_sk"],
    "income_band": ["ib_income_band_sk"],
    "promotion": ["p_promo_sk"],
    "reason": ["r_reason_sk"],
    "ship_mode": ["sm_ship_mode_sk"],
    "time_dim": ["t_time_sk"],
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
                        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
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

def convert_parquet_to_delta(spark_session: SparkSession, source_path: str, destination_path: str, optimization_technique: str, block_size: int):
    if os.path.exists(destination_path):
        return
    
    tables = [t for t in os.listdir(source_path)]

    for table in tables:
        parquet_table_path = os.path.join(source_path, table)
        delta_table_path = os.path.join(destination_path, table)
        df = spark_session.read.format('parquet').load(parquet_table_path)

        if optimization_technique == 'partitioning' and table in tpcds_partition_map:
            partition_cols = tpcds_partition_map[table]
            df = df.repartition(*partition_cols)
            writer = df.write.format('delta').mode('overwrite').partitionBy(*partition_cols)
        else:
            writer = df.write.format('delta').mode('overwrite')

        if optimization_technique == 'bloom' and table in tpcds_zorder_map:
            for col in tpcds_zorder_map[table]:
                writer.option(f"parquet.bloom.filter.enabled#{col}", "true")
                writer.option(f"parquet.bloom.filter.expected.ndv#{col}", "1000000")
        
        writer.option("parquet.block.size", f'{block_size*1048576}mb')
        writer.save(delta_table_path)

        if optimization_technique == 'zorder' and table in tpcds_zorder_map:
            dt = DeltaTable.forPath(spark_session, delta_table_path)
            z_cols = tpcds_zorder_map[table]
            dt.optimize().executeZOrderBy(*z_cols) 


def convert_parquet_to_hudi(spark_session: SparkSession, source_path: str, destination_path: str, optimization_technique: str, block_size: int):
    if os.path.exists(destination_path):
        return
    
    tables = [t for t in os.listdir(source_path)]
    hudi_uri = f'file:///{os.path.abspath(destination_path)}'

    for table in tables:
        parquet_table_path = os.path.join(source_path, table)
        hudi_table_path = os.path.join(hudi_uri, table)
        df = spark_session.read.format('parquet').load(parquet_table_path)
        pk_columns = tpcds_pk.get(table)
        hudi_record_key = ",".join(pk_columns)
        hudi_options = {
            'hoodie.table.name': table,
            'hoodie.datasource.write.recordkey.field': hudi_record_key,
            'hoodie.parquet.block.size': str(block_size*1048576),
            'hoodie.datasource.write.operation': 'bulk_insert',
        }

        match optimization_technique:
            case 'zorder':
                if table in tpcds_zorder_map:
                    z_cols = ",".join(tpcds_zorder_map[table])
                    hudi_options.update({
                        'hoodie.layout.optimize.enable': 'true',
                        'hoodie.layout.optimize.strategy': 'z-order',
                        'hoodie.layout.optimize.curve.column.names': z_cols,
                        'hoodie.bulkinsert.shuffle.parallelism': '2', 
                        'hoodie.datasource.write.row.writer.enable': 'true'
                    })
            case 'bloom':
                if table in tpcds_zorder_map:
                    z_cols = ",".join(tpcds_zorder_map[table])
                    hudi_options.update({
                        'hoodie.metadata.enable': 'true',
                        'hoodie.metadata.index.bloom.filter.enable': 'true',
                        'hoodie.metadata.index.column.stats.enable': 'true',
                        'hoodie.bloom.index.use.metadata': 'true',
                        'hoodie.metadata.index.bloom.filter.column.list': z_cols
                    })
            case 'partitioning':
                if table in tpcds_partition_map:
                    part_cols = ",".join(tpcds_partition_map[table])
                    hudi_options.update({
                        'hoodie.datasource.write.partitionpath.field': part_cols,
                        'hoodie.datasource.write.hive_style_partitioning': 'true'
                    })
            case _:
                ...

        df.write.format('hudi').options(**hudi_options).mode('overwrite').save(hudi_table_path)

def convert_parquet_to_iceberg(spark_session: SparkSession, source_path: str, namespace: str, optimization_technique: str, block_size: int):
    
    tables = [t for t in os.listdir(source_path)]
    spark_session.sql(f'CREATE NAMESPACE IF NOT EXISTS {namespace}')

    for table in tables:
        parquet_table_path = os.path.join(source_path, table)
        iceberg_table = f'{namespace}.{table}'
        df = spark_session.read.format('parquet').load(parquet_table_path)
        properties = {
            "write.parquet.row-group-size-bytes": str(block_size * 1048576)
        }

        if optimization_technique == 'bloom' and table in tpcds_zorder_map:
            for col in tpcds_zorder_map[table]:
                properties[f"write.parquet.bloom-filter-enabled.column.{col}"] = "true"

        writer = df.writeTo(iceberg_table).using('iceberg')

        if optimization_technique == 'partitioning' and table in tpcds_partition_map:
            writer = writer.partitionedBy(*tpcds_partition_map[table])

        for k, v in properties.items():
            writer = writer.tableProperty(k, v)

        writer.createOrReplace()

        if optimization_technique == 'zorder' and table in tpcds_partition_map:
            z_cols = ', '.join(tpcds_zorder_map[table])
            spark_session.sql(
                f"""CALL nessie.system.rewrite_data_files(
                    table => '{iceberg_table}', 
                    strategy => 'sort', 
                    sort_order => 'zorder({z_cols})'
                )
            """)

def get_datasource(spark_session: SparkSession, format: str, source_path: str, optimization_technique: str, block_size: int) -> str:
    match format:
        case 'delta':
            if (optimization_technique == ''):
                destination_path = f'{source_path}_{format}_{block_size}MiB'
            else:
                destination_path = f'{source_path}_{format}_{optimization_technique}_{block_size}MiB'
            convert_parquet_to_delta(spark_session=spark_session, source_path=source_path, destination_path=destination_path, optimization_technique=optimization_technique, block_size=block_size)
            return destination_path
        case 'hudi':
            data_path = os.path.abspath(source_path)
            if (optimization_technique == ''):
                destination_path = f'{data_path}_{format}_{block_size}MiB'
            else:
                destination_path = f'{data_path}_{format}_{optimization_technique}_{block_size}MiB'
            convert_parquet_to_hudi(spark_session=spark_session, source_path=data_path, destination_path=destination_path, optimization_technique=optimization_technique, block_size=block_size)
            return destination_path
        case 'iceberg':
            if (optimization_technique == ''):
                namespace = f'{source_path}_{format}_{block_size}MiB'
            else:
                namespace = f'{source_path}_{format}_{optimization_technique}_{block_size}MiB'
            convert_parquet_to_iceberg(spark_session=spark_session, source_path=source_path, namespace=namespace, optimization_technique=optimization_technique, block_size=block_size)
            return namespace
        case _:
            print('Unsupported format. Proceeding with parquet\n')
            return source_path
        
def cleanup_data(spark_session: SparkSession, format: str, path: str):
    print(f"\n{'='*40}\nInitiating cleanup for: {path}\n{'='*40}")
    try:
        if format == 'iceberg':
            shutil.rmtree(f'spark-warehouse/iceberg/{path}')
        else:
            shutil.rmtree(path)
    except Exception as e:
        print(f"Cleanup failed for {path}. Error: {e}")