import argparse
from utils import get_datasource, create_spark_session, load_data

def main(format: str, source_path: str):
    print(f"\n{'='*40}\nStarting benchmark for: {format}\n{'='*40}")
    spark_session = create_spark_session(name=f'{format}_session', format=format, master='local[*]', memory=16)    
    current_path = get_datasource(spark_session=spark_session, format=format, source_path=source_path)

    tpcds = load_data(
        spark_session=spark_session,
        data_path=current_path,
        data_format=format,
        queries=['q1'],
        namespace=(current_path if format=='iceberg' else '')
    )

    tpcds.run_TPCDS()
    tpcds.print_test_results(output_file=f'results_{format}.csv')
    spark_session.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='ODF benchmark tool',
        description='This tool enables ODF comparison with PySpark'
    )
    parser.add_argument('-f', '--format', default='parquet')
    parser.add_argument('-s', '--source_path', default='tpcds_1')
    args = parser.parse_args()
    main(**vars(args))