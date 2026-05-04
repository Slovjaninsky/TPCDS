import datetime
import os
import argparse

import pandas as pd
from utils import get_datasource, create_spark_session, load_data, cleanup_data
from sparkmeasure import StageMetrics

def main(format: str, source_path: str, queries_list: str, number_of_runs: int, queries_repeat: int, optimization_technique: str, block_size: int, memory: int, iteration: int, catalogue: str):
    print(f"\n{'='*40}\nStarting benchmark for: {format}\n{'='*40}")
    print(
f'''Data source: {source_path}
Queries: {queries_list}
Number of runs: {number_of_runs}
Queries repeat times: {queries_repeat}
Optimization technique: {optimization_technique}
Block size: {block_size}MiB'''
    )
    print(f"\n{'='*40}")
    spark_session = create_spark_session(name=f'{format}_session', format=format, master='local[*]', memory=memory)
    
    instrumentation = []
    load_stagemetrics = StageMetrics(spark_session)
    spark_session.sparkContext.setJobGroup("TPCDS", "Load data")
    startime_string = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    load_stagemetrics.begin()
    current_path = get_datasource(spark_session=spark_session, format=format, source_path=source_path, optimization_technique=optimization_technique, block_size=block_size)
    load_stagemetrics.end()
    spark_session.sparkContext.setJobGroup("", "")

    # Collect metrics and timing measurements
    load_metrics = load_stagemetrics.aggregate_stagemetrics()
    executorRunTime = round(load_metrics.get('executorRunTime') / 1000, 2)
    executorCpuTime = round(load_metrics.get('executorCpuTime') / 1000, 2)
    jvmGCTime = round(load_metrics.get('jvmGCTime') / 1000, 2)
    elapsedTime = round(load_metrics.get('elapsedTime') / 1000, 2)
    avgActiveTasks = round(load_metrics.get('executorRunTime') / load_metrics.get('elapsedTime'), 1) if load_metrics.get('elapsedTime') > 0 else 0.0

    # print the timing measurements
    print("Job finished")
    print(f"...Start Time = {startime_string}")
    print(f"...Elapsed Time = {elapsedTime} sec")
    print(f"...Executors Run Time = {executorRunTime} sec")
    print(f"...Executors CPU Time = {executorCpuTime} sec")
    print(f"...Executors JVM GC Time = {jvmGCTime} sec")
    print(f"...Average Active Tasks = {avgActiveTasks}")

    # append the timing measurements to the list
    runinfo = {'timestamp': startime_string, 'phase': 'data_loading'}
    instrumentation.append({**runinfo, **load_metrics})

    # Run the workload
    tpcds = load_data(
        spark_session=spark_session,
        data_path=current_path,
        data_format=format,
        queries=queries_list,
        num_runs=int(number_of_runs),
        queries_repeat_times=int(queries_repeat),
        namespace=(current_path if format=='iceberg' else '')
    )

    tpcds.run_TPCDS()
    
    results_dir = f'all_results/results_{iteration}/{format}/{source_path}/{optimization_technique}/{block_size}MiB'
    os.makedirs(results_dir, exist_ok=True)
    
    # Output the data loading metrics
    load_metrics_file_path = os.path.join(results_dir, 'load_metrics.csv')
    load_metrics_df = pd.DataFrame(instrumentation)
    load_metrics_df.to_csv(load_metrics_file_path, index=False)
    print(f"Data loading metrics saved to {load_metrics_file_path}")

    # Output the TPCDS metrics
    output_file_path = os.path.join(results_dir, 'results.csv')
    tpcds.print_test_results(output_file=output_file_path)
    print(f"Query execution metrics saved to {output_file_path}")

    # Cleanup folders and namespace
    cleanup_data(spark_session, format, current_path)
    
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
    parser.add_argument('-b', '--block_size', default=128)
    parser.add_argument('-m', '--memory', default=16)
    parser.add_argument('-i', '--iteration', default=0)
    parser.add_argument('-c', '--catalogue', default="http://localhost:19120/api/v1")
    args = parser.parse_args()
    main(**vars(args))