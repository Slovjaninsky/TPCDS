#!/bin/bash
set -e

SOURCES=("tpcds_1" "tpcds_10" "tpcds_100")
FORMATS=("parquet" "hudi" "delta" "iceberg")
OPTIMIZATIONS=("none" "zorder" "bloom" "partitioning")
BLOCK_SIZES=(64 128 256)

echo "=========================================="
echo "Starting TPCDS Automated Benchmark Suite"
echo "=========================================="

mkdir -p all_results

for i in $(seq 1 10); do
    for source in "${SOURCES[@]}"; do
        for format in "${FORMATS[@]}"; do
            
            if [ "$format" == "parquet" ]; then
                echo -e "\n---> Running Parquet baseline for $source"
                DATA_DIR=$source docker compose run --rm tpcds -f "$format" -s "$source" -o "none" -b 128 -i "$i" -m 56
                continue
            fi

            for opt in "${OPTIMIZATIONS[@]}"; do
                for block in "${BLOCK_SIZES[@]}"; do
                    
                    echo -e "\n---> Running Benchmark:"
                    echo "Format: $format | Source: $source | Opt: $opt | Block: ${block}MiB"
                    
                    DATA_DIR=$source docker compose run --rm tpcds \
                        -f "$format" \
                        -s "$source" \
                        -o "$opt" \
                        -b "$block" \
                        -i "$i" \
                        -m 56
                    
                    sleep 5
                done
            done
        done
    done
done

echo "=========================================="
echo "Benchmark Suite Completed Successfully!"
echo "=========================================="