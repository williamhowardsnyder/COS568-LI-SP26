#include "benchmarks/benchmark_hybrid_pgm_lipp_async.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp_async.h"

// ---------------------------------------------------------------------------
// Pareto sweep: pgm_error × flush_threshold_pct (12 combinations).
// Lower thresholds push data into LIPP faster (better for lookup-heavy).
// Higher thresholds reduce flush frequency (better for insert-heavy).
// ---------------------------------------------------------------------------
template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp_async(tli::Benchmark<uint64_t>& benchmark,
                                          bool pareto,
                                          const std::vector<int>& params) {
  if (!pareto) {
    util::fail("HybridPGMLIPPAsync hyperparameters cannot be set via params");
  }
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  16,  2>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  16,  5>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  16, 10>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  16, 20>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64,  2>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64,  5>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64, 10>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64, 20>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher, 128,  2>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher, 128,  5>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher, 128, 10>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher, 128, 20>>();
}

// ---------------------------------------------------------------------------
// Auto-select: workload-aware configs per dataset.
// 90% lookup / 10% insert → small threshold (more data in LIPP → fast lookups).
// 10% lookup / 90% insert → large threshold (fewer flushes → lower overhead).
// ---------------------------------------------------------------------------
template <int record>
void benchmark_64_hybrid_pgm_lipp_async(tli::Benchmark<uint64_t>& benchmark,
                                          const std::string& filename) {
  auto run_lookup_heavy = [&]() {
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64,  2>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64,  5>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128,  2>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128,  5>>();
  };

  auto run_insert_heavy = [&]() {
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64, 10>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64, 20>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128, 10>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128, 20>>();
  };

  bool is_lookup_heavy = filename.find("0.100000i") != std::string::npos;
  bool is_insert_heavy = filename.find("0.900000i") != std::string::npos;

  if ((filename.find("fb_100M")    != std::string::npos ||
       filename.find("books_100M") != std::string::npos ||
       filename.find("osmc_100M")  != std::string::npos) &&
      filename.find("mix") != std::string::npos) {
    if (is_lookup_heavy) run_lookup_heavy();
    else if (is_insert_heavy) run_insert_heavy();
  }
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp_async, uint64_t);
