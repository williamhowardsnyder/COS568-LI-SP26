#include "benchmarks/benchmark_hybrid_pgm_lipp_async.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp_async.h"

// ---------------------------------------------------------------------------
// Pareto sweep: pgm_error × flush_threshold_permille.
//
// Threshold is per-mille (1/1000) of initial key count.  With 100M initial
// keys: permille=1→100K, permille=3→300K, permille=5→500K, permille=10→1M.
//
// For the 90%Lkp/10%Ins workload (200K total inserts):
//   permille=1 → flush at 100K → 2 flushes; DPGM stays ≤100K entries.
//   permille=2 → flush at 200K → 1 flush.
//   permille≥5 → threshold ≥ 500K > 200K inserts → never flushes.
//
// For the 10%Lkp/90%Ins workload (1.8M total inserts):
//   permille=1  → ~18 flushes; DPGM stays ≤100K.
//   permille=3  → ~6 flushes; DPGM stays ≤300K.
//   permille=5  → ~3 flushes; DPGM stays ≤500K.
//   permille=10 → ~2 flushes; DPGM stays ≤1M.
//   permille≥20 → threshold ≥ 2M > 1.8M inserts → never flushes.
// ---------------------------------------------------------------------------
template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp_async(tli::Benchmark<uint64_t>& benchmark,
                                          bool pareto,
                                          const std::vector<int>& params) {
  if (!pareto) {
    util::fail("HybridPGMLIPPAsync hyperparameters cannot be set via params");
  }
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64,  1>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64,  2>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64,  3>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64,  5>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64, 10>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher, 128,  1>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher, 128,  2>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher, 128,  3>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher, 128,  5>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher, 128, 10>>();
}

// ---------------------------------------------------------------------------
// Auto-select: workload-aware permille values per dataset.
// ---------------------------------------------------------------------------
template <int record>
void benchmark_64_hybrid_pgm_lipp_async(tli::Benchmark<uint64_t>& benchmark,
                                          const std::string& filename) {
  // Lookup-heavy: keep DPGM tiny so bloom filter rarely fires and LIPP takes
  // all lookups.  permille=1 and 2 are the only values that trigger flushes.
  auto run_lookup_heavy = [&]() {
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64, 1>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64, 2>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128, 1>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128, 2>>();
  };

  // Insert-heavy: flush frequently so DPGM stays small during 1.8M inserts.
  auto run_insert_heavy = [&]() {
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64,  1>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64,  3>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64,  5>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64, 10>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128,  1>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128,  3>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128,  5>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128, 10>>();
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
