#ifndef INDEXES_BINARY_SEARCH_H_
#define INDEXES_BINARY_SEARCH_H_

#include <string>
#include <utility>
#include <vector>

#include "./index.h"

template <typename K, typename V>
class BinarySearch : public BaseIndex<K, V> {
 public:
  using param_t = int;
  BinarySearch(int p = 256) { record_per_page_ = p; }

  void Build(std::vector<std::pair<K, V>>& data) {
    points = std::vector<K>();
    for (size_t i = record_per_page_ - 1; i < data.size();
         i += record_per_page_) {
      points.push_back(data[i].first);
    }
    if (points.back() != data.back().first) {
      points.push_back(data.back().first);
    }

    disk_size_ = (sizeof(K) + sizeof(V)) * data.size();
  }

  SearchRange Lookup(const K lookup_key) const override {
    auto res = std::lower_bound(points.begin(), points.end(), lookup_key) -
               points.begin();

    return {res * record_per_page_, (res + 1) * record_per_page_};
  }

  size_t GetIndexParams() const override { return record_per_page_; }

  std::string GetIndexName() const override {
    return "BinarySearch_" + std::to_string(record_per_page_);
  }

  size_t GetModelNum() const override { return points.size(); }

  size_t GetInMemorySize() const override { return points.size() * sizeof(K); }

  size_t GetIndexSize() const override {
    return GetInMemorySize() + disk_size_;
  }

 private:
  std::vector<K> points;

  size_t disk_size_ = 0;
  size_t record_per_page_;
};

#endif  // INDEXES_BINARY_SEARCH_H_
