#ifndef INDEXES_RS_DISK_ORIENTED_H_
#define INDEXES_RS_DISK_ORIENTED_H_

#include <string>
#include <utility>
#include <vector>

#include "./index.h"
#include "./rs-disk/builder-disk-oriented.h"
#include "./rs-disk/radix_spline.h"

template <typename K, typename V>
class RSDiskIndex : public BaseIndex<K, V> {
 public:
  struct param_t {
    size_t num_radix_bits;
    size_t max_error;
    size_t record_per_page;
  };
  RSDiskIndex(param_t p = param_t(18, 1, 256)) {
    num_radix_bits_ = p.num_radix_bits;
    max_error_ = p.max_error;
    record_per_page_ = p.record_per_page;
  }

  void Build(std::vector<std::pair<K, V>>& data) {
    auto min = std::numeric_limits<K>::min();
    auto max = std::numeric_limits<K>::max();
    auto max_y = std::numeric_limits<V>::max();
    if (data.size() > 0) {
      min = data.front().first;
      max = data.back().first;
      max_y = data.back().second;
    }
    rs_disk_oriented::Builder<K> rsb(min, max, max_y, record_per_page_,
                                     num_radix_bits_, max_error_);
    for (const auto& kv : data) {
      rsb.AddKey(kv.first, kv.second);
    }
    rs_ = rsb.Finalize();

    disk_size_ = (sizeof(K) + sizeof(V)) * data.size();
  }

  SearchRange Lookup(const K lookup_key) const override {
    // Already exclusive in the internal algorithm
    auto range = rs_.GetSearchBound(lookup_key);
    return {range.begin, range.end};
  }

  size_t GetIndexParams() const override { return max_error_; }

  std::string GetIndexName() const override {
    return "RS-Disk-Oriented-" + std::to_string(num_radix_bits_) + "_" +
           std::to_string(max_error_);
  }

  size_t GetModelNum() const override { return rs_.GetSplineNum(); }

  size_t GetInMemorySize() const override { return rs_.GetSize(); }

  size_t GetIndexSize() const override {
    return GetInMemorySize() + disk_size_;
  }

 private:
  rs::RadixSpline<K> rs_;

  size_t num_radix_bits_;
  size_t max_error_;

  size_t disk_size_ = 0;
  size_t record_per_page_;
};

#endif  // INDEXES_RS_DISK_ORIENTED_H_
