#ifndef TLI_HYBRID_PGM_LIPP_ASYNC_H
#define TLI_HYBRID_PGM_LIPP_ASYNC_H

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <limits>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <vector>

#include "../util.h"
#include "base.h"
#include "bloom_filter.h"
#include "pgm_index_dynamic.hpp"
#include "./lipp/src/core/lipp.h"

// Async hybrid index: DPGM absorbs inserts, while immutable LIPP instances
// serve most lookups.
//
// Key design decisions:
//   1. Double-buffered DPGM: active_dpgm_ receives inserts; when it fills, it
//      is swapped into shadow_dpgm_ and flushed in the background.
//   2. Immutable LIPP layers: the initial dataset lives in base_lipp_ forever.
//      Flushed keys are bulk-loaded into a small delta_lipp_ instead of being
//      inserted one-by-one into the large base LIPP.
//   3. Bloom filters on the mutable buffers and delta LIPP: before touching an
//      expensive structure, check a cheap approximate-membership filter.
//   4. Workload-tunable flush threshold: use an absolute key-count cutoff so a
//      threshold like 64K has the same meaning on every dataset.
template <class KeyType, class SearchClass, size_t pgm_error,
          size_t flush_threshold_keys = 100000>
class HybridPGMLIPPAsync : public Competitor<KeyType, SearchClass> {
  using PGMType = PGMIndex<KeyType, SearchClass, pgm_error, 16>;
  using DPGMType = DynamicPGMIndex<KeyType, uint64_t, SearchClass, PGMType>;
  using LIPPType = LIPP<KeyType, uint64_t>;
  using KVPair = std::pair<KeyType, uint64_t>;

  struct DeltaRun {
    LIPPType lipp;
    BloomFilter bloom;
    size_t count = 0;

    size_t size_in_bytes() const {
      return lipp.index_size() + bloom.size_in_bytes();
    }
  };

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
    std::vector<KVPair> loading_data;
    loading_data.reserve(data.size());
    for (const auto& itm : data) {
      loading_data.emplace_back(itm.key, itm.value);
    }

    active_dpgm_ = DPGMType();
    {
      std::unique_lock<std::shared_mutex> sl(shadow_smutex_);
      shadow_dpgm_ = DPGMType();
      shadow_bloom_.reset();
      shadow_count_ = 0;
    }
    {
      std::unique_lock<std::shared_mutex> dl(delta_smutex_);
      delta_run_.reset();
    }

    active_count_ = 0;
    flush_threshold_ = std::max(size_t(1), flush_threshold_keys);
    flushing_.store(false, std::memory_order_release);

    size_t expected_per_flush = std::max(size_t(1000), flush_threshold_);
    active_bloom_.init(expected_per_flush);
    shadow_bloom_.init(expected_per_flush);

    uint64_t build_time = util::timing([&] {
      base_lipp_.bulk_load(loading_data.data(),
                           static_cast<int>(loading_data.size()));
    });
    return build_time;
  }

  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
    // New inserts live in the foreground DPGM.
    if (active_count_ > 0 && active_bloom_.probably_contains(lookup_key)) {
      auto it = active_dpgm_.find(lookup_key);
      if (it != active_dpgm_.end()) return it->value();
    }

    // During a flush, hold shared locks across the shadow and delta checks so
    // lookups see a consistent split of the "recently inserted" key space.
    if (flushing_.load(std::memory_order_acquire)) {
      std::shared_lock<std::shared_mutex> shadow_lock(shadow_smutex_);
      std::shared_lock<std::shared_mutex> delta_lock(delta_smutex_);

      if (shadow_count_ > 0 && shadow_bloom_.probably_contains(lookup_key)) {
        auto it = shadow_dpgm_.find(lookup_key);
        if (it != shadow_dpgm_.end()) return it->value();
      }

      if (delta_run_ && delta_run_->count > 0 &&
          delta_run_->bloom.probably_contains(static_cast<uint64_t>(lookup_key))) {
        uint64_t value;
        if (delta_run_->lipp.find(lookup_key, value)) return value;
      }
    } else {
      if (delta_run_ && delta_run_->count > 0 &&
          delta_run_->bloom.probably_contains(static_cast<uint64_t>(lookup_key))) {
        uint64_t value;
        if (delta_run_->lipp.find(lookup_key, value)) return value;
      }
    }

    uint64_t value;
    if (base_lipp_.find(lookup_key, value)) return value;
    return util::NOT_FOUND;
  }

  uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key,
                      uint32_t thread_id) const {
    uint64_t result = 0;

    auto base_it = base_lipp_.lower_bound(lower_key);
    while (base_it != base_lipp_.end() && base_it->comp.data.key <= upper_key) {
      result += base_it->comp.data.value;
      ++base_it;
    }

    auto active_it = active_dpgm_.lower_bound(lower_key);
    while (active_it != active_dpgm_.end() && active_it->key() <= upper_key) {
      result += active_it->value();
      ++active_it;
    }

    if (flushing_.load(std::memory_order_acquire)) {
      std::shared_lock<std::shared_mutex> shadow_lock(shadow_smutex_);
      std::shared_lock<std::shared_mutex> delta_lock(delta_smutex_);

      if (delta_run_) {
        auto delta_it = delta_run_->lipp.lower_bound(lower_key);
        while (delta_it != delta_run_->lipp.end() &&
               delta_it->comp.data.key <= upper_key) {
          result += delta_it->comp.data.value;
          ++delta_it;
        }
      }

      auto shadow_it = shadow_dpgm_.lower_bound(lower_key);
      while (shadow_it != shadow_dpgm_.end() &&
             shadow_it->key() <= upper_key) {
        result += shadow_it->value();
        ++shadow_it;
      }
      return result;
    }

    if (delta_run_) {
      auto delta_it = delta_run_->lipp.lower_bound(lower_key);
      while (delta_it != delta_run_->lipp.end() &&
             delta_it->comp.data.key <= upper_key) {
        result += delta_it->comp.data.value;
        ++delta_it;
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
        shadow_count_ = active_count_;
        std::swap(active_dpgm_, shadow_dpgm_);
        std::swap(active_bloom_, shadow_bloom_);
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
    size_t total = active_dpgm_.size_in_bytes() + active_bloom_.size_in_bytes() +
                   base_lipp_.index_size();
    {
      std::shared_lock<std::shared_mutex> sl(shadow_smutex_);
      total += shadow_dpgm_.size_in_bytes() + shadow_bloom_.size_in_bytes();
    }
    {
      std::shared_lock<std::shared_mutex> dl(delta_smutex_);
      if (delta_run_) total += delta_run_->size_in_bytes();
    }
    return total;
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
  static KeyType MinKey() {
    return std::numeric_limits<KeyType>::lowest();
  }

  std::shared_ptr<DeltaRun> BuildDeltaRunFromShadowAndPrevious() const {
    std::shared_ptr<DeltaRun> previous_delta;
    {
      std::shared_lock<std::shared_mutex> dl(delta_smutex_);
      previous_delta = delta_run_;
    }

    std::vector<KVPair> merged;

    std::shared_lock<std::shared_mutex> sl(shadow_smutex_);
    merged.reserve((previous_delta ? previous_delta->count : 0) + shadow_count_);

    auto shadow_it =
        shadow_count_ == 0 ? shadow_dpgm_.end() : shadow_dpgm_.lower_bound(MinKey());
    auto shadow_end = shadow_dpgm_.end();

    if (previous_delta) {
      auto delta_it = previous_delta->lipp.lower_bound(MinKey());
      auto delta_end = previous_delta->lipp.end();

      while (delta_it != delta_end && shadow_it != shadow_end) {
        const KeyType delta_key = delta_it->comp.data.key;
        const KeyType shadow_key = shadow_it->key();

        if (delta_key < shadow_key) {
          merged.emplace_back(delta_key, delta_it->comp.data.value);
          ++delta_it;
        } else if (shadow_key < delta_key) {
          merged.emplace_back(shadow_key, shadow_it->value());
          ++shadow_it;
        } else {
          // Newer flushed data wins if a duplicate ever slips in.
          merged.emplace_back(shadow_key, shadow_it->value());
          ++delta_it;
          ++shadow_it;
        }
      }

      while (delta_it != delta_end) {
        merged.emplace_back(delta_it->comp.data.key, delta_it->comp.data.value);
        ++delta_it;
      }
    }

    while (shadow_it != shadow_end) {
      merged.emplace_back(shadow_it->key(), shadow_it->value());
      ++shadow_it;
    }

    sl.unlock();

    if (merged.empty()) return nullptr;

    auto next_delta = std::make_shared<DeltaRun>();
    next_delta->count = merged.size();
    next_delta->bloom.init(std::max(size_t(1000), next_delta->count));
    for (const auto& kv : merged) {
      next_delta->bloom.insert(static_cast<uint64_t>(kv.first));
    }
    next_delta->lipp.bulk_load(merged.data(), static_cast<int>(merged.size()));
    return next_delta;
  }

  void PublishDeltaRun(std::shared_ptr<DeltaRun> next_delta) {
    std::unique_lock<std::shared_mutex> shadow_lock(shadow_smutex_);
    std::unique_lock<std::shared_mutex> delta_lock(delta_smutex_);

    delta_run_ = std::move(next_delta);
    shadow_dpgm_ = DPGMType();
    shadow_bloom_.reset();
    shadow_count_ = 0;
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
        auto next_delta = BuildDeltaRunFromShadowAndPrevious();
        PublishDeltaRun(std::move(next_delta));
        flushing_.store(false, std::memory_order_release);
      }

      if (shutdown_.load(std::memory_order_relaxed)) break;
    }
  }

  DPGMType active_dpgm_;
  mutable DPGMType shadow_dpgm_;

  mutable LIPPType base_lipp_;
  mutable std::shared_ptr<DeltaRun> delta_run_;

  BloomFilter active_bloom_;
  mutable BloomFilter shadow_bloom_;

  mutable std::shared_mutex shadow_smutex_;
  mutable std::shared_mutex delta_smutex_;

  std::atomic<bool> shutdown_;
  std::atomic<bool> flushing_;

  std::thread flush_thread_;
  std::mutex flush_cv_mutex_;
  std::condition_variable flush_cv_;

  size_t active_count_ = 0;
  size_t shadow_count_ = 0;
  size_t flush_threshold_ = 0;
};

#endif  // TLI_HYBRID_PGM_LIPP_ASYNC_H
