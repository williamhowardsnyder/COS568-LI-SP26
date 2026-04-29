#ifndef TLI_HYBRID_PGM_LIPP_ASYNC_H
#define TLI_HYBRID_PGM_LIPP_ASYNC_H

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <vector>

#include "../util.h"
#include "base.h"
#include "bloom_filter.h"
#include "pgm_index_dynamic.hpp"
#include "./lipp/src/core/lipp.h"

// Async hybrid index: DPGM absorbs inserts, LIPP serves lookups.
//
// Key design decisions:
//   1. Double-buffered DPGM: active_dpgm_ receives inserts; when it hits the
//      flush threshold it is atomically swapped with shadow_dpgm_ and a
//      background thread drains shadow→LIPP without blocking the client.
//   2. Bloom filters on each DPGM buffer: before doing an expensive DPGM
//      traversal, we check the corresponding bloom filter. A "definitely not
//      here" answer lets us skip the DPGM entirely (~10ns vs ~200ns).
//   3. Lock-free LIPP reads on the fast path: the background thread only
//      writes to LIPP when flushing_=true. When flushing_=false the client
//      can read LIPP without acquiring any lock.
//   4. Fixed flush threshold: use an absolute key-count cutoff rather than a
//      threshold that grows with total_keys_. This keeps workload tuning
//      predictable: a 32K threshold always means "flush after ~32K inserts."
template <class KeyType, class SearchClass, size_t pgm_error,
          size_t flush_threshold_keys = 100000>
class HybridPGMLIPPAsync : public Competitor<KeyType, SearchClass> {
  using PGMType  = PGMIndex<KeyType, SearchClass, pgm_error, 16>;
  using DPGMType = DynamicPGMIndex<KeyType, uint64_t, SearchClass, PGMType>;

 public:
  HybridPGMLIPPAsync(const std::vector<int>& params)
      : shutdown_(false), flushing_(false) {
    flush_thread_ = std::thread(&HybridPGMLIPPAsync::RunFlushThread, this);
  }

  ~HybridPGMLIPPAsync() {
    {
      std::lock_guard<std::mutex> lk(flush_cv_mutex_);
      shutdown_.store(true, std::memory_order_relaxed);
    }
    flush_cv_.notify_one();
    if (flush_thread_.joinable()) flush_thread_.join();
  }

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
    std::vector<std::pair<KeyType, uint64_t>> loading_data;
    loading_data.reserve(data.size());
    for (const auto& itm : data) {
      loading_data.emplace_back(itm.key, itm.value);
    }
    active_count_ = 0;
    flush_threshold_ = std::max(size_t(1), flush_threshold_keys);

    // Size bloom filters for the expected number of keys per flush cycle.
    size_t expected_per_flush = std::max(size_t(1000), flush_threshold_);
    active_bloom_.init(expected_per_flush);
    shadow_bloom_.init(expected_per_flush);

    uint64_t build_time =
        util::timing([&] { lipp_.bulk_load(loading_data.data(), loading_data.size()); });
    return build_time;
  }

  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
    // -----------------------------------------------------------------------
    // Fast path: no flush in progress.
    //
    // The background thread only writes to LIPP when flushing_=true. Loading
    // flushing_=false with acquire semantics guarantees all previous LIPP
    // writes are visible and no new ones will start until Insert() sets it
    // back to true (which cannot happen concurrently since the client is
    // single-threaded for !multithread workloads).  So we read LIPP without
    // any lock.
    // -----------------------------------------------------------------------
    if (!flushing_.load(std::memory_order_acquire)) {
      // Only touch active_dpgm_ if the bloom filter says the key might be there.
      if (active_count_ > 0 && active_bloom_.probably_contains(lookup_key)) {
        auto it = active_dpgm_.find(lookup_key);
        if (it != active_dpgm_.end()) return it->value();
      }
      // Lock-free LIPP read — safe because flushing_=false.
      uint64_t value;
      if (lipp_.find(lookup_key, value)) return value;
      return util::NOT_FOUND;
    }

    // -----------------------------------------------------------------------
    // Slow path: flush in progress — use full synchronization.
    // -----------------------------------------------------------------------

    // Check active DPGM (client thread only — no lock needed).
    if (active_count_ > 0 && active_bloom_.probably_contains(lookup_key)) {
      auto it = active_dpgm_.find(lookup_key);
      if (it != active_dpgm_.end()) return it->value();
    }

    // Check shadow DPGM — background thread is reading it concurrently.
    {
      std::shared_lock<std::shared_mutex> sl(shadow_smutex_);
      if (shadow_bloom_.probably_contains(lookup_key)) {
        auto it2 = shadow_dpgm_.find(lookup_key);
        if (it2 != shadow_dpgm_.end()) return it2->value();
      }
    }

    // Check LIPP with shared lock.
    {
      std::shared_lock<std::shared_mutex> rl(lipp_mutex_);
      uint64_t value;
      if (lipp_.find(lookup_key, value)) return value;
    }

    return util::NOT_FOUND;
  }

  uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key,
                      uint32_t thread_id) const {
    uint64_t result = 0;

    if (!flushing_.load(std::memory_order_acquire)) {
      // Fast path: lock-free LIPP read + active DPGM.
      auto lipp_it = lipp_.lower_bound(lower_key);
      while (lipp_it != lipp_.end() && lipp_it->comp.data.key <= upper_key) {
        result += lipp_it->comp.data.value;
        ++lipp_it;
      }
      auto dpgm_it = active_dpgm_.lower_bound(lower_key);
      while (dpgm_it != active_dpgm_.end() && dpgm_it->key() <= upper_key) {
        result += dpgm_it->value();
        ++dpgm_it;
      }
      return result;
    }

    // Slow path.
    {
      std::shared_lock<std::shared_mutex> rl(lipp_mutex_);
      auto it = lipp_.lower_bound(lower_key);
      while (it != lipp_.end() && it->comp.data.key <= upper_key) {
        result += it->comp.data.value;
        ++it;
      }
    }
    {
      auto it = active_dpgm_.lower_bound(lower_key);
      while (it != active_dpgm_.end() && it->key() <= upper_key) {
        result += it->value();
        ++it;
      }
    }
    {
      std::shared_lock<std::shared_mutex> sl(shadow_smutex_);
      auto it = shadow_dpgm_.lower_bound(lower_key);
      while (it != shadow_dpgm_.end() && it->key() <= upper_key) {
        result += it->value();
        ++it;
      }
    }
    return result;
  }

  void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {
    active_dpgm_.insert(data.key, data.value);
    active_bloom_.insert(static_cast<uint64_t>(data.key));
    ++active_count_;

    if (active_count_ >= flush_threshold_ &&
        !flushing_.load(std::memory_order_acquire)) {
      {
        std::unique_lock<std::shared_mutex> sl(shadow_smutex_);
        std::swap(active_dpgm_, shadow_dpgm_);
        std::swap(active_bloom_, shadow_bloom_);
        // active_bloom_ is now the old (reset) shadow_bloom_; reset for safety.
        active_bloom_.reset();
      }
      active_count_ = 0;
      flushing_.store(true, std::memory_order_release);
      {
        std::lock_guard<std::mutex> lk(flush_cv_mutex_);
        flush_cv_.notify_one();
      }
    }
  }

  std::string name() const { return "HybridPGMLIPPAsync"; }

  std::size_t size() const {
    return active_dpgm_.size_in_bytes() + shadow_dpgm_.size_in_bytes() +
           lipp_.index_size() + active_bloom_.size_in_bytes() +
           shadow_bloom_.size_in_bytes();
  }

  bool applicable(bool unique, bool range_query, bool insert, bool multithread,
                  const std::string& ops_filename) const {
    std::string sname = SearchClass::name();
    return unique && !multithread && sname != "LinearAVX";
  }

  std::vector<std::string> variants() const {
    return {SearchClass::name(), std::to_string(pgm_error),
            std::to_string(flush_threshold_keys)};
  }

 private:
  // Amortize lock/unlock overhead across a small chunk of migrated keys
  // without blocking slow-path lookups for an entire flush.
  static constexpr size_t kFlushBatchSize = 256;

  void RunFlushThread() {
    while (true) {
      bool do_flush = false;
      {
        std::unique_lock<std::mutex> lk(flush_cv_mutex_);
        flush_cv_.wait(lk, [this] {
          return flushing_.load(std::memory_order_relaxed) ||
                 shutdown_.load(std::memory_order_relaxed);
        });
        do_flush = flushing_.load(std::memory_order_relaxed);
      }

      if (do_flush) {
        // Drain shadow_dpgm_ into lipp_ in chunks. This keeps the number of
        // expensive write-lock acquisitions low without fully monopolizing
        // LIPP for the whole flush.
        {
          std::shared_lock<std::shared_mutex> sl(shadow_smutex_);
          auto it = shadow_dpgm_.lower_bound(KeyType(0));
          while (it != shadow_dpgm_.end()) {
            std::unique_lock<std::shared_mutex> wl(lipp_mutex_);
            for (size_t batch = 0;
                 batch < kFlushBatchSize && it != shadow_dpgm_.end();
                 ++batch, ++it) {
              lipp_.insert(it->key(), it->value());
            }
          }
        }

        // Reset shadow structures under exclusive lock.
        {
          std::unique_lock<std::shared_mutex> sl(shadow_smutex_);
          shadow_dpgm_ = DPGMType();
          shadow_bloom_.reset();
        }

        flushing_.store(false, std::memory_order_release);
      }

      if (shutdown_.load(std::memory_order_relaxed)) break;
    }
  }

  DPGMType         active_dpgm_;
  mutable DPGMType shadow_dpgm_;
  mutable LIPP<KeyType, uint64_t> lipp_;

  BloomFilter         active_bloom_;
  mutable BloomFilter shadow_bloom_;

  mutable std::shared_mutex lipp_mutex_;
  mutable std::shared_mutex shadow_smutex_;

  std::atomic<bool> shutdown_;
  std::atomic<bool> flushing_;

  std::thread             flush_thread_;
  std::mutex              flush_cv_mutex_;
  std::condition_variable flush_cv_;

  size_t active_count_    = 0;
  size_t flush_threshold_ = 0;
};

#endif  // TLI_HYBRID_PGM_LIPP_ASYNC_H
