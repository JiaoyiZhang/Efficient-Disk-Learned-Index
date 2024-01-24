#ifndef INDEXES_PGM_INDEX_DISK_PG_H_
#define INDEXES_PGM_INDEX_DISK_PG_H_

#include <string>
#include <utility>
#include <vector>

#include "./PGM-index-disk/pgm_index_page.hpp"
#include "./index.h"

template <typename K, typename V>
class PGMIndexPagePG : public BaseIndex<K, V> {
 public:
  struct param_t {
    size_t epsilon_;
    size_t pred_gran_;
  };
  PGMIndexPagePG(param_t p = param_t(2, 1))
      : epsilon_(p.epsilon_), pred_gran_(p.pred_gran_) {}

  void Build(std::vector<std::pair<K, V>>& data) {
    if (pred_gran_ > 2) {
      std::vector<std::pair<K, V>> sparse_data(data.size() / pred_gran_ * 2);
      sparse_data[0] = data[0];
      size_t cnt = pred_gran_, idx = 1;
      while (cnt < data.size()) {
        sparse_data[idx++] = data[cnt - 1];
        sparse_data[idx++] = data[cnt];
        cnt += pred_gran_;
      }
      if (cnt == data.size()) {
        sparse_data[idx++] = data[cnt - 1];
      }
      pgm_page_pg_ = pgm_page::PGMIndexPage<K>(sparse_data.begin(),
                                               sparse_data.end(), epsilon_);
    } else {
      pgm_page_pg_ =
          pgm_page::PGMIndexPage<K>(data.begin(), data.end(), epsilon_);
    }
    disk_size_ = (sizeof(K) + sizeof(V)) * data.size();
  }

  SearchRange Lookup(const K lookup_key) const override {
    auto range = pgm_page_pg_.search(lookup_key);
    if (range.lo >= 1) {
      range.lo -= 1;
    }
    return {range.lo, range.hi + 1};
  }

  std::string GetIndexName() const override {
    return "PGM Index Page PG_" + std::to_string(epsilon_);
  }

  size_t GetIndexParams() const override { return epsilon_; }

  size_t GetModelNum() const override { return pgm_page_pg_.segments_count(); }

  size_t GetInMemorySize() const override {
    return pgm_page_pg_.size_in_bytes();
  }

  size_t GetIndexSize() const override {
    return GetInMemorySize() + disk_size_;
  }

 private:
  pgm_page::PGMIndexPage<K> pgm_page_pg_;
  size_t disk_size_ = 0;
  size_t epsilon_;
  size_t pred_gran_;
};

#endif  // INDEXES_PGM_INDEX_DISK_PG_H_
