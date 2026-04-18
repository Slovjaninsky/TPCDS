#!/bin/bash
set -e

SOURCES=("tpcds_1" "tpcds_10" "tpcds_100")
FORMATS=( "hudi" "delta" "iceberg")
OPTIMIZATIONS=("none" "zorder" "bloom" "partitioning")
BLOCK_SIZES=(64 128 256)

echo "=========================================="
echo "Starting TPCDS Automated Benchmark Suite"
echo "=========================================="

echo -e "\n---> Activating venv"
source .venv/bin/activate

for source in "${SOURCES[@]}"; do
    for format in "${FORMATS[@]}"; do
        
        if [ "$format" == "parquet" ]; then
            echo -e "\n---> Running Parquet baseline for $source"
            python3 main.py -f "$format" -s "$source" -o "none" -b 128
            continue
        fi

        for opt in "${OPTIMIZATIONS[@]}"; do
            for block in "${BLOCK_SIZES[@]}"; do
                
                echo -e "\n---> Running Benchmark:"
                echo "Format: $format | Source: $source | Opt: $opt | Block: ${block}MiB"
                
                python3 main.py \
                    -f "$format" \
                    -s "$source" \
                    -o "$opt" \
                    -b "$block"
                
                sleep 5
            done
        done
    done
done

echo "=========================================="
echo "Benchmark Suite Completed Successfully!"
echo "=========================================="