#!/usr/bin/env bash
# Run Milestone 2 benchmarks: compare DPGM, LIPP, and HybridPGMLIPP on
# the two mixed workloads (10% insert / 90% insert) for Facebook dataset.
# For a full sweep add books_100M_public_uint64 and osmc_100M_public_uint64.

set -e

BENCHMARK=build/benchmark
if [ ! -f $BENCHMARK ]; then
    echo "benchmark binary does not exist — run build_benchmark.sh first"
    exit 1
fi

mkdir -p ./results

function run_mixed() {
    local DATA=$1   # e.g. fb_100M_public_uint64
    local INDEX=$2  # e.g. DynamicPGM

    echo "[$INDEX] 90% Lookup / 10% Insert  ($DATA)"
    $BENCHMARK ./data/$DATA \
        ./data/${DATA}_ops_2M_0.000000rq_0.500000nl_0.100000i_0m_mix \
        --through --csv --only $INDEX -r 3

    echo "[$INDEX] 10% Lookup / 90% Insert  ($DATA)"
    $BENCHMARK ./data/$DATA \
        ./data/${DATA}_ops_2M_0.000000rq_0.500000nl_0.900000i_0m_mix \
        --through --csv --only $INDEX -r 3
}

# Milestone 2: Facebook dataset only
for INDEX in DynamicPGM LIPP HybridPGMLIPP; do
    run_mixed fb_100M_public_uint64 $INDEX
done

echo "=== Hybrid benchmark complete ==="

# Add CSV headers
for FILE in ./results/*mix*.csv; do
    if head -n 1 "$FILE" | grep -q "index_name"; then
        sed -i '1d' "$FILE"
    fi
    sed -i '1s/^/index_name,build_time_ns1,build_time_ns2,build_time_ns3,index_size_bytes,mixed_throughput_mops1,mixed_throughput_mops2,mixed_throughput_mops3,search_method,value,flush_threshold_pct\n/' "$FILE"
    echo "Header set for $FILE"
done
