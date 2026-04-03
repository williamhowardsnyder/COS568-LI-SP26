#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>

#include "../util.h"
#include "base.h"
#include "pgm_index_dynamic.hpp"
#include "./lipp/src/core/lipp.h"

// Hybrid index: DPGM absorbs inserts, LIPP serves lookups.
// When DPGM accumulates flush_threshold_pct% of total keys, we flush
// its contents into LIPP via individual inserts (naive strategy).
template <class KeyType, class SearchClass, size_t pgm_error, size_t flush_threshold_pct = 5>
class HybridPGMLIPP : public Competitor<KeyType, SearchClass> {
  using PGMType = PGMIndex<KeyType, SearchClass, pgm_error, 16>;
  using DPGMType = DynamicPGMIndex<KeyType, uint64_t, SearchClass, PGMType>;

 public:
  HybridPGMLIPP(const std::vector<int>& params) {}

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
    std::vector<std::pair<KeyType, uint64_t>> loading_data;
    loading_data.reserve(data.size());
    for (const auto& itm : data) {
      loading_data.emplace_back(itm.key, itm.value);
    }

    total_keys_  = data.size();
    dpgm_count_  = 0;
    // Flush when DPGM holds flush_threshold_pct% of all keys seen so far.
    // Re-calculated after each flush to reflect the growing total.
    UpdateThreshold();

    uint64_t build_time =
        util::timing([&] { lipp_.bulk_load(loading_data.data(), loading_data.size()); });

    return build_time;
  }

  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
    // Check DPGM first (holds the most recent inserts).
    auto it = pgm_.find(lookup_key);
    if (it != pgm_.end()) {
      return it->value();
    }

    // Fall back to LIPP.
    uint64_t value;
    if (lipp_.find(lookup_key, value)) {
      return value;
    }

    return util::NOT_FOUND;
  }

  uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key,
                      uint32_t thread_id) const {
    uint64_t result = 0;

    // LIPP range
    auto lipp_it = lipp_.lower_bound(lower_key);
    while (lipp_it != lipp_.end() && lipp_it->comp.data.key <= upper_key) {
      result += lipp_it->comp.data.value;
      ++lipp_it;
    }

    // DPGM range
    auto pgm_it = pgm_.lower_bound(lower_key);
    while (pgm_it != pgm_.end() && pgm_it->key() <= upper_key) {
      result += pgm_it->value();
      ++pgm_it;
    }

    return result;
  }

  void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {
    pgm_.insert(data.key, data.value);
    ++dpgm_count_;
    ++total_keys_;

    if (dpgm_count_ >= flush_threshold_) {
      Flush();
    }
  }

  std::string name() const { return "HybridPGMLIPP"; }

  std::size_t size() const { return pgm_.size_in_bytes() + lipp_.index_size(); }

  bool applicable(bool unique, bool range_query, bool insert, bool multithread,
                  const std::string& ops_filename) const {
    std::string sname = SearchClass::name();
    // LIPP requires unique keys; LinearAVX is unsupported.
    return unique && !multithread && sname != "LinearAVX";
  }

  std::vector<std::string> variants() const {
    return {SearchClass::name(), std::to_string(pgm_error),
            std::to_string(flush_threshold_pct)};
  }

 private:
  void UpdateThreshold() {
    flush_threshold_ =
        std::max(size_t(1), total_keys_ * flush_threshold_pct / 100);
  }

  // Naive flush: iterate DPGM in sorted order, insert each item into LIPP.
  void Flush() {
    for (auto it = pgm_.begin(); it != pgm_.end(); ++it) {
      lipp_.insert(it->key(), it->value());
    }
    // Reset DPGM to an empty container.
    pgm_ = DPGMType();
    dpgm_count_ = 0;
    UpdateThreshold();
  }

  DPGMType pgm_;
  LIPP<KeyType, uint64_t> lipp_;
  size_t total_keys_     = 0;
  size_t dpgm_count_     = 0;
  size_t flush_threshold_ = 0;
};

#endif  // TLI_HYBRID_PGM_LIPP_H
