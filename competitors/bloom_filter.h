#ifndef TLI_BLOOM_FILTER_H
#define TLI_BLOOM_FILTER_H

#include <cmath>
#include <cstdint>
#include <cstring>
#include <vector>

// Cache-line-resident (blocked) Bloom filter for uint64_t keys.
//
// A standard Bloom filter probes k random positions across the full bit array,
// causing k cache misses per lookup.  A blocked Bloom filter maps ALL k probes
// for a single key into ONE 512-bit (64-byte = one cache line) block, so every
// lookup costs exactly ONE cache miss regardless of filter size or k.
//
// Trade-off: blocked filters need ~1.5x more bits for the same FPR.  We
// compensate in init() so the effective false-positive rate stays near `fpr`.
class BloomFilter {
  static constexpr size_t kBitsPerBlock  = 512;
  static constexpr size_t kWordsPerBlock = kBitsPerBlock / 64;  // 8 uint64_ts
  static constexpr size_t kNumHashes    = 6;

 public:
  BloomFilter() : num_blocks_(0), mask_(0) {}

  // Size the filter for `expected_elements` keys at FPR ≈ fpr.
  void init(size_t expected_elements, double fpr = 0.01) {
    if (expected_elements == 0) expected_elements = 1;
    // Blocked BF needs ~1.5x bits vs. standard BF for the same FPR.
    size_t raw_bits = static_cast<size_t>(
        std::ceil(-1.5 * expected_elements * std::log(fpr) /
                  (std::log(2.0) * std::log(2.0))));
    size_t raw_blocks = (raw_bits + kBitsPerBlock - 1) / kBitsPerBlock;
    // Power-of-2 number of blocks → fast bitwise-AND for block selection.
    num_blocks_ = 1;
    while (num_blocks_ < raw_blocks) num_blocks_ <<= 1;
    mask_ = num_blocks_ - 1;
    bits_.assign(num_blocks_ * kWordsPerBlock, 0ULL);
  }

  void insert(uint64_t key) {
    uint64_t h1, h2;
    hashes(key, h1, h2);
    uint64_t* base = block_ptr(h1);
    // All kNumHashes probes stay within the same 64-byte block (1 cache miss).
    for (size_t i = 0; i < kNumHashes; ++i) {
      const size_t bit =
          static_cast<size_t>(h2 + i * (h1 | 1)) & (kBitsPerBlock - 1);
      base[bit >> 6] |= (1ULL << (bit & 63));
    }
  }

  bool probably_contains(uint64_t key) const {
    if (num_blocks_ == 0) return false;
    uint64_t h1, h2;
    hashes(key, h1, h2);
    const uint64_t* base = block_ptr(h1);
    for (size_t i = 0; i < kNumHashes; ++i) {
      const size_t bit =
          static_cast<size_t>(h2 + i * (h1 | 1)) & (kBitsPerBlock - 1);
      if (!(base[bit >> 6] & (1ULL << (bit & 63)))) return false;
    }
    return true;
  }

  void reset() { std::fill(bits_.begin(), bits_.end(), 0ULL); }

  size_t size_in_bytes() const { return bits_.size() * 8; }

 private:
  static void hashes(uint64_t key, uint64_t& h1, uint64_t& h2) {
    h1 = mix64(key);
    h2 = mix64(key ^ 0x9e3779b97f4a7c15ULL);
  }

  uint64_t* block_ptr(uint64_t h1) {
    return bits_.data() + (h1 & mask_) * kWordsPerBlock;
  }
  const uint64_t* block_ptr(uint64_t h1) const {
    return bits_.data() + (h1 & mask_) * kWordsPerBlock;
  }

  // Finalizer-quality 64-bit mixer (splitmix64).
  static uint64_t mix64(uint64_t x) {
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
  }

  std::vector<uint64_t> bits_;
  size_t num_blocks_;
  size_t mask_;
};

#endif  // TLI_BLOOM_FILTER_H
