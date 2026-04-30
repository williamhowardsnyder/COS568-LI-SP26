#include "benchmarks/benchmark_hybrid_pgm_lipp_async.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp_async.h"

// ---------------------------------------------------------------------------
// Pareto sweep: pgm_error × fixed flush_threshold_keys.
//
// Absolute thresholds make the hybrid much easier to reason about than a
// moving "percent of total keys" rule:
//   32K  → frequent flushes, very small DPGM
//   64K  → moderate lookup-heavy setting
//   128K → a single flush on the 90%Lkp/10%Ins workload
//   256K+→ no flush at all on the 90%Lkp/10%Ins workload
// ---------------------------------------------------------------------------
template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp_async(tli::Benchmark<uint64_t>& benchmark,
                                          bool pareto,
                                          const std::vector<int>& params) {
  if (!pareto) {
    util::fail("HybridPGMLIPPAsync hyperparameters cannot be set via params");
  }
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64,  32768>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64,  65536>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64, 131072>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64, 262144>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64, 524288>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher, 128,  32768>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher, 128,  65536>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher, 128, 131072>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher, 128, 262144>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher, 128, 524288>>();
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
  // Lookup-heavy: with 200K total inserts, only thresholds ≤200K trigger any
  // flush.  Thresholds above 200K mean no flush → no delta_run_ → single bloom
  // check per lookup.  Also test 300K/400K to confirm the no-flush sweet spot.
  auto run_lookup_heavy = [&]() {
    // pgm_error=64,128: original configs
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64, 262144>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64, 393216>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128, 262144>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128, 393216>>();
    // pgm_error=256,512,1024: larger segments → fewer DPGM merges → lower insert cost
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 256, 262144>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 256, 393216>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 512, 262144>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 512, 393216>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 1024, 262144>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 1024, 393216>>();
  };

  // Insert-heavy: larger buffers reduce flush churn, but still trigger enough
  // migrations during 1.8M inserts to exercise the hybrid design.
  auto run_insert_heavy = [&]() {
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64,  65536>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64, 131072>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64, 262144>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64, 524288>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128,  65536>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128, 131072>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128, 262144>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128, 524288>>();
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
