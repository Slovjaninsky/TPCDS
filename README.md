# TPC-DS PySpark executor with Open Data Format (Delta, Hudi, Iceberg)

This TPC-DS executor allows to run TPC-DS workloads with different Open Data Formats as a part of a benchmark.

### Prerequisites
Python 3.11.x,

Libraries list in `requirements.txt`,

Additionally, to run workloads on Iceberg, the [Nessie](https://projectnessie.org/guides/docker/) catalogue engine should be running on `localhost:19120`.

### Workload execution
`main.py [-h] [-f FORMAT] [-s SOURCE_PATH] [-q QUERIES_LIST] [-n NUMBER_OF_RUNS] [-r QUERIES_REPEAT] [-o OPTIMIZATION_TECHNIQUE]`

Formats: `parquet` (default), `delta`, `hudi`, `iceberg`.

Source: the directory of the data. Data can be found on [https://sparkdltrigger.web.cern.ch/sparkdltrigger/TPCDS/tpcds_10.zip](https://sparkdltrigger.web.cern.ch/sparkdltrigger/TPCDS/tpcds_10.zip), or generated with a dedicated TPC-DS generation tool. Default: `tpcds_1`
.

Queries: `all` for all 99 queries, or comma-separated list of queries ids `"q1, q2, q3"`. Default: `all`.

Number of runs: integer, number of runs for the workload. Default: `1`.

Queries repeat: integer, number of times the query repeats during the worklaod. Default: `1`.

Optimization technique: string. Default: none.

Available optimization techniques:

* Delta (`zorder`, `compaction`)
* Hudi (`bloom`, `global_bloom`, `simple`, `global_simple`, `hbase`, `inmemory`, `bucket_simple`, `record_level_index`, `global_record_level_index`)
* Iceberg (`zorder`, `bloom`)

For example, `main.py -f delta -s tpcds_10 -q all`, `main.py -f hudi -o bloom` or `main.py -f iceberg -s tpcds_10 -q "q1, q2, q3" -n 3 -r 5`