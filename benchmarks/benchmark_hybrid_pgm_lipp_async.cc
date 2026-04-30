#include "benchmarks/benchmark_hybrid_pgm_lipp_async.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp_async.h"

// ---------------------------------------------------------------------------
// Pareto sweep: pgm_error × fixed flush_threshold_keys × bloom_fpr.
//
// Absolute thresholds make the hybrid much easier to reason about than a
// moving "percent of total keys" rule:
//   32K  → frequent flushes, very small DPGM
//   64K  → moderate lookup-heavy setting
//   128K → a single flush on the 90%Lkp/10%Ins workload
//   256K+→ no flush at all on the 90%Lkp/10%Ins workload
//
// Bloom FPR is encoded "per thousand":
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
  // Lookup-heavy: cover both "keep the DPGM tiny" and "flush infrequently"
  // regimes, since the README explicitly warns that too-frequent flushing may
  // hurt this workload.
  // Thresholds above 200K mean "no flush" on this workload, which is useful
  // right now because it isolates the active bloom/DPGM front-buffer cost.
  // We sweep a coarse set of FPRs here to see whether the current bottleneck is
  // false positives rather than migration frequency.
  auto run_lookup_heavy = [&]() {
    run_hybrid_async_fpr_sweep<BranchingBinarySearch<record>, 64, 262144>(
        benchmark);
    run_hybrid_async_fpr_sweep<BranchingBinarySearch<record>, 64, 393216>(
        benchmark);
    run_hybrid_async_fpr_sweep<BranchingBinarySearch<record>, 128, 262144>(
        benchmark);
    run_hybrid_async_fpr_sweep<BranchingBinarySearch<record>, 128, 393216>(
        benchmark);
    run_hybrid_async_fpr_sweep<BranchingBinarySearch<record>, 256, 262144>(
        benchmark);
    run_hybrid_async_fpr_sweep<BranchingBinarySearch<record>, 256, 393216>(
        benchmark);
    run_hybrid_async_fpr_sweep<BranchingBinarySearch<record>, 512, 262144>(
        benchmark);
    run_hybrid_async_fpr_sweep<BranchingBinarySearch<record>, 512, 393216>(
        benchmark);
    run_hybrid_async_fpr_sweep<BranchingBinarySearch<record>, 1024, 262144>(
        benchmark);
    run_hybrid_async_fpr_sweep<BranchingBinarySearch<record>, 1024, 393216>(
        benchmark);
  };

  // Insert-heavy: larger buffers reduce flush churn, but still trigger enough
  // migrations during 1.8M inserts to exercise the hybrid design.  Keep a
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
