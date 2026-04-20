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
#include "pgm_index_dynamic.hpp"
#include "./lipp/src/core/lipp.h"

// Async hybrid index: DPGM absorbs inserts, LIPP serves lookups.
//
// Double-buffered DPGM design: active_dpgm_ receives all new inserts.
// When it reaches flush_threshold_pct% of total keys, it is atomically swapped
// with shadow_dpgm_ and a background thread is woken to drain shadow→LIPP.
// The client thread is never blocked waiting for a flush.
template <class KeyType, class SearchClass, size_t pgm_error,
          size_t flush_threshold_pct = 5>
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
    total_keys_   = data.size();
    active_count_ = 0;
    UpdateThreshold();

    uint64_t build_time =
        util::timing([&] { lipp_.bulk_load(loading_data.data(), loading_data.size()); });
    return build_time;
  }

  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
    // 1. Check active DPGM — only the client thread writes here; no lock needed.
    auto it = active_dpgm_.find(lookup_key);
    if (it != active_dpgm_.end()) return it->value();

    // 2. Check shadow DPGM if a flush is in progress.
    //    Items exist in shadow until the background resets it, so checking here
    //    before LIPP guarantees we don't miss keys mid-flight.
    if (flushing_.load(std::memory_order_acquire)) {
      std::shared_lock<std::shared_mutex> sl(shadow_smutex_);
      auto it2 = shadow_dpgm_.find(lookup_key);
      if (it2 != shadow_dpgm_.end()) return it2->value();
    }

    // 3. Check LIPP (shared lock; background flush holds exclusive lock per insert).
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

    if (flushing_.load(std::memory_order_acquire)) {
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
    ++active_count_;
    ++total_keys_;
    UpdateThreshold();

    // Trigger async flush when threshold is hit and no flush is in progress.
    // If a flush is already running, active_dpgm_ grows beyond threshold until
    // it completes — inserts are never blocked.
    if (active_count_ >= flush_threshold_ &&
        !flushing_.load(std::memory_order_acquire)) {
      {
        std::unique_lock<std::shared_mutex> sl(shadow_smutex_);
        std::swap(active_dpgm_, shadow_dpgm_);
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
           lipp_.index_size();
  }

  bool applicable(bool unique, bool range_query, bool insert, bool multithread,
                  const std::string& ops_filename) const {
    std::string sname = SearchClass::name();
    return unique && !multithread && sname != "LinearAVX";
  }

  std::vector<std::string> variants() const {
    return {SearchClass::name(), std::to_string(pgm_error),
            std::to_string(flush_threshold_pct)};
  }

 private:
  void UpdateThreshold() {
    flush_threshold_ = std::max(size_t(1), total_keys_ * flush_threshold_pct / 100);
  }

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
        // Drain shadow_dpgm_ into lipp_, releasing lipp_mutex_ between items
        // so client lookups can interleave with the background flush.
        {
          std::shared_lock<std::shared_mutex> sl(shadow_smutex_);
          // lower_bound(0) avoids the missing 3-arg iterator constructor in
          // DynamicPGMIndex::begin(); semantically equivalent for uint64_t keys.
          auto it = shadow_dpgm_.lower_bound(KeyType(0));
          for (; it != shadow_dpgm_.end(); ++it) {
            std::unique_lock<std::shared_mutex> wl(lipp_mutex_);
            lipp_.insert(it->key(), it->value());
          }
        }

        // Reset shadow under exclusive lock so client reads don't race with
        // the DPGMType destructor/constructor.
        {
          std::unique_lock<std::shared_mutex> sl(shadow_smutex_);
          shadow_dpgm_ = DPGMType();
        }

        // Release order: all shadow writes are visible before flushing_=false.
        flushing_.store(false, std::memory_order_release);
      }

      if (shutdown_.load(std::memory_order_relaxed)) break;
    }
  }

  // active_dpgm_: client thread only (Insert + EqualityLookup sequential).
  // shadow_dpgm_: read by client (lookup) and background (drain); written
  //               during swap (Insert) and reset (background). Protected by
  //               shadow_smutex_.
  DPGMType             active_dpgm_;
  mutable DPGMType     shadow_dpgm_;
  mutable LIPP<KeyType, uint64_t> lipp_;

  mutable std::shared_mutex lipp_mutex_;
  mutable std::shared_mutex shadow_smutex_;

  std::atomic<bool> shutdown_;
  std::atomic<bool> flushing_;

  std::thread             flush_thread_;
  std::mutex              flush_cv_mutex_;
  std::condition_variable flush_cv_;

  size_t total_keys_      = 0;
  size_t active_count_    = 0;
  size_t flush_threshold_ = 0;
};

#endif  // TLI_HYBRID_PGM_LIPP_ASYNC_H
