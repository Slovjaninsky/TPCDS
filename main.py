import os
from CustomTPCDS import CustomTPCDS
from pyspark.sql import SparkSession

def create_spark_session(name: str) -> SparkSession:
    spark_session = SparkSession.builder \
        .appName(name) \
        .config("spark.jars.packages", "ch.cern.sparkmeasure:spark-measure_2.12:0.27") \
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


def main():
    spark_session = create_spark_session(name="Test run")

    tpcds = load_data(
        spark_session=spark_session,
        data_path='tpcds_10',
        data_format="parquet",
        queries=['q1', 'q2', 'q3']
    )

    tpcds.run_TPCDS()
    tpcds.print_test_results(output_file='results.csv')

if __name__ == '__main__':
    main()