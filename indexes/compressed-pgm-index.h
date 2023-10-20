
#ifndef INDEXES_COMPRESSED_PGM_INDEX_H_
#define INDEXES_COMPRESSED_PGM_INDEX_H_

#include <string>
#include <utility>
#include <vector>

#include "./PGM-index-disk/pgm_index_variants.hpp"
#include "./index.h"

template <typename K, typename V>
class CompressedPGM : public BaseIndex<K, V> {
 public:
  using param_t = int;
  CompressedPGM(int eps = 64) : epsilon_(eps) {}

  void Build(std::vector<std::pair<K, V>>& data) {
    std::vector<K> keys = std::vector<K>(data.size());
    int idx = 0;
    for (auto record : data) {
      keys[idx++] = record.first;
    }
    pgm_page_ =
        pgm_page::CompressedPGMIndex<K>(keys.begin(), keys.end(), epsilon_);
    disk_size_ = (sizeof(K) + sizeof(V)) * data.size();
  }

  SearchRange Lookup(const K lookup_key) const override {
    auto range = pgm_page_.search(lookup_key);
    return {range.lo, range.hi};
  }

  std::string GetIndexName() const override {
    return "CompressedPGM_" + std::to_string(epsilon_);
  }

  size_t GetIndexParams() const override { return epsilon_; }

  size_t GetModelNum() const override { return pgm_page_.segments_count(); }

  size_t GetInMemorySize() const override { return pgm_page_.size_in_bytes(); }

  size_t GetIndexSize() const override {
    return GetInMemorySize() + disk_size_;
  }

 private:
  pgm_page::CompressedPGMIndex<K> pgm_page_;
  size_t disk_size_ = 0;
  size_t epsilon_;
};

#endif  // INDEXES_COMPRESSED_PGM_INDEX_H_
