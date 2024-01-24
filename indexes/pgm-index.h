#ifndef INDEXES_PGM_INDEX_H_
#define INDEXES_PGM_INDEX_H_

#include <string>
#include <utility>
#include <vector>

#include "./PGM-index/include/pgm/pgm_index.hpp"
#include "./index.h"

template <typename K, typename V, int epsilon = 64>
class PGMIndex : public BaseIndex<K, V> {
 public:
  using param_t = int;
  PGMIndex(int pg = 1) : pred_gran_(pg) {}

  void Build(std::vector<std::pair<K, V>>& data) {
    std::vector<K> keys = std::vector<K>(data.size());
    int idx = 0;
    for (auto record : data) {
      keys[idx++] = record.first;
    }
    pgm_ = pgm::PGMIndex<K, epsilon>(keys.begin(), keys.end());
    disk_size_ = (sizeof(K) + sizeof(V)) * data.size();
    max_value_ = data.back().second;

#ifdef PRINT_BOUND
    std::string filename = GetIndexName() + ".csv";
    std::ofstream outBound(filename, std::ios::app | std::ios::out);
    for (int i = 0; i < data.size(); i++) {
      auto range = pgm_.search(data[i].first);
      range.lo /= pred_gran_;
      range.hi /= pred_gran_;
      if (range.hi > max_value_) {
        range.hi = max_value_;
      }
      outBound << i << ", , " << (range.hi - range.lo) << ", "
               << std::abs(i - int(range.lo)) << std::endl;
    }
#endif
#ifdef PRINT_INFO
    uint64_t diff_sum = 0;
    for (int i = 0; i < data.size(); i++) {
      auto range = pgm_.search(data[i].first);
      range.lo /= pred_gran_;
      range.hi /= pred_gran_;
      if (range.hi > max_value_) {
        range.hi = max_value_;
      }
      diff_sum += std::abs(i - int(range.lo));
    }
    double diff_avg = diff_sum * 1.0 / data.size();
    std::cout << "diff_avg," << diff_avg << std::endl;
#endif
  }

  SearchRange Lookup(const K lookup_key) const override {
    auto range = pgm_.search(lookup_key);
    range.lo /= pred_gran_;
    range.hi /= pred_gran_;
    if (range.hi > max_value_) {
      range.hi = max_value_;
    }
    return {range.lo, range.hi + 1};
  }

  size_t GetIndexParams() const override { return epsilon; }

  std::string GetIndexName() const override {
    return "PGM Index_" + std::to_string(epsilon);
  }

  size_t GetModelNum() const override { return pgm_.segments_count(); }

  size_t GetInMemorySize() const override { return pgm_.size_in_bytes(); }

  size_t GetIndexSize() const override {
    return GetInMemorySize() + disk_size_;
  }

 private:
  pgm::PGMIndex<K, epsilon> pgm_;
  size_t disk_size_ = 0;
  uint64_t max_value_;
  int pred_gran_ = 1;
};

#endif  // INDEXES_PGM_INDEX_H_
