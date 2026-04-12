import argparse
from utils import get_datasource, create_spark_session, load_data

def main(format: str, source_path: str, queries_list: str, number_of_runs: int, queries_repeat: int, optimization_technique: str, block_size: int):
    print(f"\n{'='*40}\nStarting benchmark for: {format}\n{'='*40}")
    print(
f'''Data source: {source_path}
Queries: {queries_list}
Number of runs: {number_of_runs}
Queries repeat times: {queries_repeat}
Optimization technique: {optimization_technique}
Block size: {block_size}'''
    )
    print(f"\n{'='*40}")
    spark_session = create_spark_session(name=f'{format}_session', format=format, master='local[*]', memory=16)    
    current_path = get_datasource(spark_session=spark_session, format=format, source_path=source_path, optimization_technique=optimization_technique, block_size=block_size)

    tpcds = load_data(
        spark_session=spark_session,
        data_path=current_path,
        data_format=format,
        queries=queries_list,
        num_runs=number_of_runs,
        queries_repeat_times=queries_repeat,
        namespace=(current_path if format=='iceberg' else '')
    )

    tpcds.run_TPCDS()
    tpcds.print_test_results(output_file=f'results_{format}_{source_path}_{optimization_technique}.csv')
    spark_session.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='main.py',
        description='This tool enables ODF comparison with PySpark'
    )
    parser.add_argument('-f', '--format', default='parquet')
    parser.add_argument('-s', '--source_path', default='tpcds_1')
    parser.add_argument('-q', '--queries-list', default='all')
    parser.add_argument('-n', '--number_of_runs', default=1)
    parser.add_argument('-r', '--queries_repeat', default=1)
    parser.add_argument('-o', '--optimization_technique', default='')
    parser.add_argument('-b', '--block_size', default=134217728)
    args = parser.parse_args()
    main(**vars(args))