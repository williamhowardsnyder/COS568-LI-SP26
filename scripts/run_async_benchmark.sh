#!/usr/bin/env bash
# Milestone 3 benchmark: compare DynamicPGM, LIPP, HybridPGMLIPP (M2 naive),
# and HybridPGMLIPPAsync (M3 async) on the two mixed workloads across all
# three datasets (Facebook, Books, OSMC).

set -e

BENCHMARK=build/benchmark
if [ ! -f $BENCHMARK ]; then
    echo "benchmark binary does not exist — run build_benchmark.sh first"
    exit 1
fi

mkdir -p ./results

DATASETS=(
    "fb_100M_public_uint64"
    "books_100M_public_uint64"
    "osmc_100M_public_uint64"
)

INDEXES=(
    "DynamicPGM"
    "LIPP"
    "HybridPGMLIPP"
    "HybridPGMLIPPAsync"
)

function run_mixed() {
    local DATA=$1
    local INDEX=$2

    echo "[$INDEX] 90% Lookup / 10% Insert  ($DATA)"
    $BENCHMARK ./data/$DATA \
        ./data/${DATA}_ops_2M_0.000000rq_0.500000nl_0.100000i_0m_mix \
        --through --csv --only $INDEX -r 3

    echo "[$INDEX] 10% Lookup / 90% Insert  ($DATA)"
    $BENCHMARK ./data/$DATA \
        ./data/${DATA}_ops_2M_0.000000rq_0.500000nl_0.900000i_0m_mix \
        --through --csv --only $INDEX -r 3
}

for DATA in "${DATASETS[@]}"; do
    for INDEX in "${INDEXES[@]}"; do
        run_mixed "$DATA" "$INDEX"
    done
done

echo "=== Async benchmark complete ==="
