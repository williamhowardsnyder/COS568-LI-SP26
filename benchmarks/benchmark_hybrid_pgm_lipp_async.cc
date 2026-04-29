#include "benchmarks/benchmark_hybrid_pgm_lipp_async.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp_async.h"

// ---------------------------------------------------------------------------
// Pareto sweep: pgm_error × fixed flush_threshold_keys.
//
// Absolute thresholds make the hybrid much easier to reason about than a
// moving "percent of total keys" rule:
//   8K   → keep DPGM tiny, but flush very frequently
//   32K  → moderate lookup-heavy setting
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
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64,   8192>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64,  16384>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64,  32768>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64,  65536>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64, 131072>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64, 262144>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher,  64, 524288>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher, 128,   8192>>();
  benchmark.template Run<HybridPGMLIPPAsync<uint64_t, Searcher, 128,  16384>>();
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
  // Lookup-heavy: try genuinely small write buffers so lookups mostly see a
  // near-empty DPGM and flushes happen at predictable points.
  auto run_lookup_heavy = [&]() {
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64,   8192>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64,  16384>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64,  32768>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>,  64,  65536>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128,   8192>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128,  16384>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128,  32768>>();
    benchmark.template Run<HybridPGMLIPPAsync<uint64_t, BranchingBinarySearch<record>, 128,  65536>>();
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
