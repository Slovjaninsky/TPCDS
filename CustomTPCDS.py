from tpcds_pyspark import TPCDS
from importlib.resources import files

class CustomTPCDS(TPCDS):
    def tpcds_queries(self):
        return [
            'q1', 'q2', 'q3', 'q4', 'q5', 'q5a', 'q6', 'q7', 'q8', 'q9',
            'q10', 'q10a', 'q11', 'q12', 'q13', 'q14a', 'q14b', 'q14', 'q15',
            'q16', 'q17', 'q18', 'q18a', 'q19', 'q20', 'q21', 'q22', 'q22a',
            'q23a', 'q23b', 'q24', 'q24a', 'q24b', 'q25', 'q26', 'q27', 'q27a',
            'q28', 'q29', 'q30', 'q31', 'q32', 'q33', 'q34', 'q35', 'q35a',
            'q36', 'q36a', 'q37', 'q38', 'q39a', 'q39b', 'q40', 'q41', 'q42',
            'q43', 'q44', 'q45', 'q46', 'q47', 'q48', 'q49', 'q50', 'q51',
            'q51a', 'q52', 'q53', 'q54', 'q55', 'q56', 'q57', 'q58', 'q59',
            'q60', 'q61', 'q62', 'q63', 'q64', 'q65', 'q66', 'q67', 'q67a',
            'q68', 'q69', 'q70', 'q70a', 'q71', 'q72', 'q73', 'q74', 'q75',
            'q76', 'q77', 'q77a', 'q78', 'q79', 'q80', 'q80a', 'q81', 'q82',
            'q83', 'q84', 'q85', 'q86', 'q86a', 'q87', 'q88', 'q89', 'q90',
            'q91', 'q92', 'q93', 'q94', 'q95', 'q96', 'q97', 'q98', 'q99'
        ]

    def __init__(
        self,
        spark_session, 
        data_path, 
        data_format,
        num_runs, 
        queries_repeat_times, 
        queries,
        queries_exclude=[], 
        sleep_time=1
    ):
        self.spark = spark_session
        self.data_path = data_path
        self.data_format = data_format
        self.queries = self.tpcds_queries() if queries.lower() == 'all' else [q.strip() for q in queries.split(',')]
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
        print(self.queries)
        if not set(self.queries).issubset(self.tpcds_queries()):
            raise ValueError(f"queries must be a subset of {self.tpcds_queries()}")
        # Check that queries_exclude is a subset of queries
        if not set(queries_exclude).issubset(self.queries):
            raise ValueError(f"queries_exclude must be a subset of {self.queries}")
        # Subtract queries_exclude from queries, keeping the order of queries
        self.queries_to_run = [query for query in self.queries if query not in queries_exclude]

    def map_tables_iceberg(self, namespace='', define_temporary_views=True):
        """Modification of the map_table function for TPCDS for Iceberg compatibility
        And we don't create catalogues since they are already inplace after conversion"""

        data_format = self.data_format
        spark = self.spark
        tables = self.tpcds_tables

        spark.sql(f'USE {namespace}')
        # Loop through each table name and create a temporary view for it
        if define_temporary_views:
            for table in tables:
                print(f"Creating temporary view {table}")
                spark.read.format(data_format).load(f'{namespace}.{table}').createOrReplaceTempView(table)