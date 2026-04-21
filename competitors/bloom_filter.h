#ifndef TLI_BLOOM_FILTER_H
#define TLI_BLOOM_FILTER_H

#include <cmath>
#include <cstdint>
#include <cstring>
#include <vector>

// Simple Bloom filter for uint64_t keys.
// Uses the Kirsch-Mitzenmacher double-hashing trick to simulate k independent
// hash functions from two base hashes, avoiding the cost of k separate hash
// computations.
//
// Bit array is sized to the next power-of-2 so that the position calculation
// uses a fast bitwise AND instead of an expensive integer division.  With k≈7
// hash functions, this saves ~7 divisions (≈30-40 cycles each) per probe.
class BloomFilter {
 public:
  BloomFilter() : num_bits_(0), mask_(0), num_hashes_(0) {}

  // Size the filter for `expected_elements` keys at false-positive rate `fpr`.
  void init(size_t expected_elements, double fpr = 0.01) {
    if (expected_elements == 0) expected_elements = 1;
    // Optimal bit count: m = -n * ln(fpr) / (ln 2)^2
    size_t raw_bits = static_cast<size_t>(
        std::ceil(-1.0 * expected_elements * std::log(fpr) /
                  (std::log(2.0) * std::log(2.0))));
    // Round up to next power-of-2 so (pos & mask_) replaces (pos % num_bits_).
    num_bits_ = 1;
    while (num_bits_ < raw_bits) num_bits_ <<= 1;
    mask_ = num_bits_ - 1;
    // Optimal hash count: k = (m/n) * ln 2 — cap at 8 to bound probe cost.
    num_hashes_ = std::min(size_t(8), std::max(size_t(1),
        static_cast<size_t>(std::round(
            (double)num_bits_ / expected_elements * std::log(2.0)))));
    bits_.assign(num_bits_ / 64, 0ULL);
  }

  void insert(uint64_t key) {
    uint64_t h1 = mix64(key);
    uint64_t h2 = mix64(key ^ 0xdeadbeefcafeULL);
    for (size_t i = 0; i < num_hashes_; ++i) {
      size_t pos = (h1 + i * h2) & mask_;
      bits_[pos >> 6] |= (1ULL << (pos & 63));
    }
  }

  bool probably_contains(uint64_t key) const {
    if (num_bits_ == 0) return false;
    uint64_t h1 = mix64(key);
    uint64_t h2 = mix64(key ^ 0xdeadbeefcafeULL);
    for (size_t i = 0; i < num_hashes_; ++i) {
      size_t pos = (h1 + i * h2) & mask_;
      if (!(bits_[pos >> 6] & (1ULL << (pos & 63)))) return false;
    }
    return true;
  }

  void reset() { std::fill(bits_.begin(), bits_.end(), 0ULL); }

  size_t size_in_bytes() const { return bits_.size() * 8; }

 private:
  // Finalizer-quality 64-bit mixer (from splitmix64).
  static uint64_t mix64(uint64_t x) {
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
  }

  std::vector<uint64_t> bits_;
  size_t num_bits_;
  size_t mask_;       // num_bits_ - 1; valid because num_bits_ is a power-of-2
  size_t num_hashes_;
};

#endif  // TLI_BLOOM_FILTER_H
