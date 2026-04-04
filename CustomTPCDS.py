from tpcds_pyspark import TPCDS
from importlib.resources import files

class CustomTPCDS(TPCDS):

    def __init__(
        self,
        spark_session=None, 
        data_path="./tpcds_10", 
        data_format="parquet",
        num_runs=2, 
        queries_repeat_times=3, 
        queries=[], 
        queries_exclude=[], 
        sleep_time=1
    ):
        self.spark = spark_session
        self.data_path = data_path
        self.data_format = data_format
        self.queries = queries
        self.queries_repeat_times = queries_repeat_times
        self.num_runs = num_runs
        self.sleep_time = sleep_time
        self.start_time = None
        self.end_time = None
        self.test_results = None
        self.results_pdf = None
        self.grouped_results_pdf = None

        # Path to the TPCDS queries on the filesystem
        tpcds_pyspark_files = files('tpcds_pyspark')
        self.queries_path = tpcds_pyspark_files.joinpath('Queries') # Path to the TPCDS queries on the filesystem
        print(f"TPCDS queries path: {self.queries_path}")

        # Input validation for the queries and queries_exclude
        # Check that queries is equal or a subset of tpcds_queries
        if not set(queries).issubset(self.tpcds_queries):
            raise ValueError(f"queries must be a subset of {self.tpcds_queries}")
        # Check that queries_exclude is a subset of queries
        if not set(queries_exclude).issubset(self.queries):
            raise ValueError(f"queries_exclude must be a subset of {self.queries}")
        # Subtract queries_exclude from queries, keeping the order of queries
        self.queries_to_run = [query for query in self.queries if query not in queries_exclude]