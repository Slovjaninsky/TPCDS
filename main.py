import os
from utils import get_datasource, create_spark_session, load_data

def main():
    data_path='tpcds_10'
    formats = ['delta']
    spark_session = create_spark_session(name="Test run")

    for format in formats:
        
        current_path = get_datasource(spark_session=spark_session, format=format, source_path=data_path, destination_path=f'{data_path}_{format}')

        tpcds = load_data(
            spark_session=spark_session,
            data_path=current_path,
            data_format=format,
            queries=['q1']
        )

        tpcds.run_TPCDS()
        tpcds.print_test_results(output_file=f'results_{format}.csv')

if __name__ == '__main__':
    main()