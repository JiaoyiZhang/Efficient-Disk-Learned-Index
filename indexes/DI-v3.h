#ifndef INDEXES_DI_V3_H_
#define INDEXES_DI_V3_H_

#include <string>
#include <utility>
#include <vector>

#include "./Compressed-Disk-Oriented-Index/di_v3.h"
#include "./index.h"

template <typename K, typename V>
class DI_V3 : public BaseIndex<K, V> {
 public:
  struct param_t {
    float lambda;
    size_t record_per_page;
  };

  DI_V3(param_t p = param_t(1.25, 256)) {
    lambda_ = p.lambda;
    record_per_page_ = p.record_per_page;
  }

  void Build(std::vector<std::pair<K, V>>& data) {
    di_ = compressed_disk_index::DiskOrientedIndexV3<K, V>(record_per_page_);
    di_.Build(data, lambda_);

    disk_size_ = (sizeof(K) + sizeof(V)) * data.size();
  }

  SearchRange Lookup(const K lookup_key) const override {}

  SearchRange Lookup(const K lookup_key) {
    auto range = di_.GetSearchBound(lookup_key);
    return {range.begin, range.end};
  }

  size_t GetIndexParams() const override { return lambda_; }

  std::string GetIndexName() const override {
    auto str0 = std::to_string(lambda_);
    return "DI-V3_" + str0.substr(0, str0.find(".") + 3);
  }

  size_t GetModelNum() const override { return di_.GetModelNum(); }

  size_t GetInMemorySize() const override { return di_.GetSize(); }

  size_t GetIndexSize() const override {
    return GetInMemorySize() + disk_size_;
  }

 private:
  compressed_disk_index::DiskOrientedIndexV3<K, V> di_;

  float lambda_;
  size_t disk_size_ = 0;
  size_t record_per_page_;
};

#endif  // INDEXES_DI_V3_H_
