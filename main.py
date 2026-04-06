import os
from utils import get_datasource, create_spark_session, load_data

def main():
    source_path = 'tpcds_1'
    formats = ['iceberg']

    for format in formats:
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

if __name__ == '__main__':
    main()