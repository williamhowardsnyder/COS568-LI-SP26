#include "benchmarks/benchmark_hybrid_pgm_lipp.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

// ---------------------------------------------------------------------------
// Pareto sweep: vary both pgm_error and flush threshold.
// ---------------------------------------------------------------------------
template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                   bool pareto,
                                   const std::vector<int>& params) {
  if (!pareto) {
    util::fail("HybridPGMLIPP hyperparameters cannot be set via params");
  }
  // pgm_error x flush_threshold_pct grid
  benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16,  2>>();
  benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16,  5>>();
  benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16, 10>>();
  benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 64,  2>>();
  benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 64,  5>>();
  benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 64, 10>>();
  benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 128, 2>>();
  benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 128, 5>>();
  benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 128,10>>();
}

// ---------------------------------------------------------------------------
// Auto-select: pick best-known configurations per dataset / workload.
// Only the Facebook dataset is required for Milestone 2.
// ---------------------------------------------------------------------------
template <int record>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                   const std::string& filename) {
  if (filename.find("fb_100M") != std::string::npos) {
    if (filename.find("mix") != std::string::npos) {
      if (filename.find("0.100000i") != std::string::npos) {
        // 90% lookup, 10% insert — prefer smaller flush threshold so that
        // LIPP (fast lookup) holds the majority of data.
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64,  5>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 10>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128, 5>>();
      } else if (filename.find("0.900000i") != std::string::npos) {
        // 10% lookup, 90% insert — more inserts, so DPGM is used heavily.
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64,  2>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64,  5>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128, 5>>();
      } else if (filename.find("0.500000i") != std::string::npos) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64,  5>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128, 5>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 256, 5>>();
      }
    }
  }

  if (filename.find("books_100M") != std::string::npos) {
    if (filename.find("mix") != std::string::npos) {
      if (filename.find("0.100000i") != std::string::npos) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64,  5>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 10>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128, 5>>();
      } else if (filename.find("0.900000i") != std::string::npos) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64,  2>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64,  5>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128, 5>>();
      }
    }
  }

  if (filename.find("osmc_100M") != std::string::npos) {
    if (filename.find("mix") != std::string::npos) {
      if (filename.find("0.100000i") != std::string::npos) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64,  5>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 10>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128, 5>>();
      } else if (filename.find("0.900000i") != std::string::npos) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64,  2>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64,  5>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128, 5>>();
      }
    }
  }
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);
