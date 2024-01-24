#ifndef INDEXES_PGM_INDEX_DISK_H_
#define INDEXES_PGM_INDEX_DISK_H_

#include <string>
#include <utility>
#include <vector>

#include "./PGM-index-disk/pgm_index_page.hpp"
#include "./index.h"

template <typename K, typename V>
class PGMIndexPage : public BaseIndex<K, V> {
 public:
  using param_t = int;
  PGMIndexPage(int eps = 2) : epsilon_(eps) {}

  void Build(std::vector<std::pair<K, V>>& data) {
    pgm_page_ = pgm_page::PGMIndexPage<K>(data.begin(), data.end(), epsilon_);
    disk_size_ = (sizeof(K) + sizeof(V)) * data.size();
#ifdef PRINT_INFO
    uint64_t diff_sum = 0;
    for (int i = 0; i < data.size(); i++) {
      auto range = pgm_page_.search(data[i].first);
      range.lo *= 512;
      diff_sum += std::abs(i - int(range.lo));
    }
    double diff_avg = diff_sum * 1.0 / data.size();
    std::cout << "diff_avg," << diff_avg << std::endl;
#endif
  }

  SearchRange Lookup(const K lookup_key) const override {
    auto range = pgm_page_.search(lookup_key);
    return {range.lo, range.hi};
  }

  std::string GetIndexName() const override {
    return "PGM Index Page_" + std::to_string(epsilon_);
  }

  size_t GetIndexParams() const override { return epsilon_; }

  size_t GetModelNum() const override { return pgm_page_.segments_count(); }

  size_t GetInMemorySize() const override { return pgm_page_.size_in_bytes(); }

  size_t GetIndexSize() const override {
    return GetInMemorySize() + disk_size_;
  }

 private:
  pgm_page::PGMIndexPage<K> pgm_page_;
  size_t disk_size_ = 0;
  size_t epsilon_;
};

#endif  // INDEXES_PGM_INDEX_DISK_H_
