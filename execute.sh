#!/bin/bash
# set -e

SOURCES=("tpcds_100" "tpcds_1" "tpcds_10")
FORMATS=("parquet" "hudi" "delta" "iceberg")
OPTIMIZATIONS=("none" "zorder" "bloom" "partitioning")
BLOCK_SIZES=(64 128 256)
MEMORY=32
REPEAT_TIMES=10

echo "=========================================="
echo "Starting TPCDS Automated Benchmark Suite"
echo "=========================================="

source .venv/bin/activate
mkdir -p all_results

for source in "${SOURCES[@]}"; do
    for format in "${FORMATS[@]}"; do
        
        if [ "$format" == "parquet" ]; then
            echo -e "\n---> Running Parquet baseline for $source"
            # DATA_DIR=$source docker compose run --rm tpcds -f "$format" -s "$source" -o "none" -b 128 -m "$MEMORY"
            python3 main.py -f "$format" -s "$source" -o "none" -b 128 -n $REPEAT_TIMES -m $MEMORY
            continue
        fi

        for opt in "${OPTIMIZATIONS[@]}"; do
            for block in "${BLOCK_SIZES[@]}"; do
                
                echo -e "\n---> Running Benchmark:"
                echo "Format: $format | Source: $source | Opt: $opt | Block: ${block}MiB"
                
                # DATA_DIR=$source docker compose run --rm tpcds \
                #     -f "$format" \
                #     -s "$source" \
                #     -o "$opt" \
                #     -b "$block" \
                #     -m "$MEMORY"

                python3 main.py \
                    -f "$format" \
                    -s "$source" \
                    -o "$opt" \
                    -b $block \
                    -n $REPEAT_TIMES \
                    -m $MEMORY
                
                sleep 5
            done
        done
    done
done

echo "=========================================="
echo "Benchmark Suite Completed Successfully!"
echo "=========================================="