#include "benchmarks/benchmark_hybrid_pgm_lipp_async.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp_async.h"

// ---------------------------------------------------------------------------
// Pareto sweep: pgm_error × fixed flush_threshold_keys × bloom_fpr.
//
// Bloom FPR is encoded "per thousand":
//   1   → 0.1%
//   2   → 0.2%
//   5   → 0.5%
//   10  → 1.0%
//   50  → 5.0%
//   200 → 20.0%
// ---------------------------------------------------------------------------
template <typename Searcher, size_t pgm_error, size_t flush_threshold_keys>
void run_hybrid_async_fpr_sweep(tli::Benchmark<uint64_t>& benchmark) {
  benchmark.template Run<
      HybridPGMLIPPAsync<uint64_t, Searcher, pgm_error, flush_threshold_keys>>(
      {5});
  benchmark.template Run<
      HybridPGMLIPPAsync<uint64_t, Searcher, pgm_error, flush_threshold_keys>>(
      {10});
  benchmark.template Run<
      HybridPGMLIPPAsync<uint64_t, Searcher, pgm_error, flush_threshold_keys>>(
      {50});
  benchmark.template Run<
      HybridPGMLIPPAsync<uint64_t, Searcher, pgm_error, flush_threshold_keys>>(
      {200});
}

template <typename Searcher, size_t pgm_error, size_t flush_threshold_keys>
void run_hybrid_async_lookup_fpr_sweep(tli::Benchmark<uint64_t>& benchmark) {
  benchmark.template Run<
      HybridPGMLIPPAsync<uint64_t, Searcher, pgm_error, flush_threshold_keys>>(
      {1});
  benchmark.template Run<
      HybridPGMLIPPAsync<uint64_t, Searcher, pgm_error, flush_threshold_keys>>(
      {2});
  benchmark.template Run<
      HybridPGMLIPPAsync<uint64_t, Searcher, pgm_error, flush_threshold_keys>>(
      {5});
  benchmark.template Run<
      HybridPGMLIPPAsync<uint64_t, Searcher, pgm_error, flush_threshold_keys>>(
      {10});
}

template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp_async(tli::Benchmark<uint64_t>& benchmark,
                                          bool pareto,
                                          const std::vector<int>& params) {
  if (!pareto && !params.empty()) {
    util::fail("HybridPGMLIPPAsync runtime params are only supported in pareto mode");
  }
  run_hybrid_async_fpr_sweep<Searcher, 64, 32768>(benchmark);
  run_hybrid_async_fpr_sweep<Searcher, 64, 65536>(benchmark);
  run_hybrid_async_fpr_sweep<Searcher, 64, 131072>(benchmark);
  run_hybrid_async_fpr_sweep<Searcher, 64, 262144>(benchmark);
  run_hybrid_async_fpr_sweep<Searcher, 64, 524288>(benchmark);
  run_hybrid_async_fpr_sweep<Searcher, 128, 32768>(benchmark);
  run_hybrid_async_fpr_sweep<Searcher, 128, 65536>(benchmark);
  run_hybrid_async_fpr_sweep<Searcher, 128, 131072>(benchmark);
  run_hybrid_async_fpr_sweep<Searcher, 128, 262144>(benchmark);
  run_hybrid_async_fpr_sweep<Searcher, 128, 524288>(benchmark);
}

// ---------------------------------------------------------------------------
// Auto-select: workload-aware fixed thresholds per dataset.
// ---------------------------------------------------------------------------
template <int record>
void benchmark_64_hybrid_pgm_lipp_async(tli::Benchmark<uint64_t>& benchmark,
                                          const std::string& filename) {
  // Lookup-heavy: keep the sweep focused on the current best region:
  // higher pgm_error values, no-flush thresholds, and low bloom FPRs.
  // On the 90% lookup / 10% insert workload, 262144 and 393216 both avoid
  // flushing during the measured window, which isolates the front-buffer cost.
  auto run_lookup_heavy = [&]() {
    run_hybrid_async_lookup_fpr_sweep<BranchingBinarySearch<record>, 256,
                                      262144>(benchmark);
    run_hybrid_async_lookup_fpr_sweep<BranchingBinarySearch<record>, 256,
                                      393216>(benchmark);
    run_hybrid_async_lookup_fpr_sweep<BranchingBinarySearch<record>, 512,
                                      262144>(benchmark);
    run_hybrid_async_lookup_fpr_sweep<BranchingBinarySearch<record>, 512,
                                      393216>(benchmark);
    run_hybrid_async_lookup_fpr_sweep<BranchingBinarySearch<record>, 1024,
                                      262144>(benchmark);
    run_hybrid_async_lookup_fpr_sweep<BranchingBinarySearch<record>, 1024,
                                      393216>(benchmark);
    run_hybrid_async_lookup_fpr_sweep<BranchingBinarySearch<record>, 2048,
                                      262144>(benchmark);
    run_hybrid_async_lookup_fpr_sweep<BranchingBinarySearch<record>, 2048,
                                      393216>(benchmark);
    run_hybrid_async_lookup_fpr_sweep<BranchingBinarySearch<record>, 4096,
                                      262144>(benchmark);
    run_hybrid_async_lookup_fpr_sweep<BranchingBinarySearch<record>, 4096,
                                      393216>(benchmark);
  };

  // Insert-heavy: larger buffers reduce flush churn, but still trigger enough
  // migrations during 1.8M inserts to exercise the hybrid design. Keep a
  // smaller FPR sweep here so we do not explode the total benchmark count.
  auto run_insert_heavy = [&]() {
    benchmark.template Run<
        HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 64, 65536>>(
        {10});
    benchmark.template Run<
        HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 64, 65536>>(
        {50});
    benchmark.template Run<
        HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 64,
                           131072>>({10});
    benchmark.template Run<
        HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 64,
                           131072>>({50});
    benchmark.template Run<
        HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 64,
                           262144>>({10});
    benchmark.template Run<
        HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 64,
                           262144>>({50});
    benchmark.template Run<
        HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 64,
                           524288>>({10});
    benchmark.template Run<
        HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 64,
                           524288>>({50});
    benchmark.template Run<
        HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128,
                           65536>>({10});
    benchmark.template Run<
        HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128,
                           65536>>({50});
    benchmark.template Run<
        HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128,
                           131072>>({10});
    benchmark.template Run<
        HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128,
                           131072>>({50});
    benchmark.template Run<
        HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128,
                           262144>>({10});
    benchmark.template Run<
        HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128,
                           262144>>({50});
    benchmark.template Run<
        HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128,
                           524288>>({10});
    benchmark.template Run<
        HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128,
                           524288>>({50});
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
