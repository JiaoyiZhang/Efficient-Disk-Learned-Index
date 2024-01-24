#ifndef INDEXES_RS_DISK_PG_H_
#define INDEXES_RS_DISK_PG_H_

#include <string>
#include <utility>
#include <vector>

#include "./index.h"
#include "./rs-disk/builder.h"
#include "./rs-disk/radix_spline.h"

template <typename K, typename V>
class RSPGIndex : public BaseIndex<K, V> {
 public:
  struct param_t {
    size_t num_radix_bits;
    size_t max_error;
    size_t pred_gran_;
  };
  RSPGIndex(param_t p = param_t(18, 1, 1)) {
    num_radix_bits_ = p.num_radix_bits;
    max_error_ = p.max_error;
    pred_gran_ = p.pred_gran_;
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
    rs::Builder<K> rsb(min, max, max_y, num_radix_bits_, max_error_);
    if (pred_gran_ > 2) {
      rsb.AddKey(data[0].first, data[0].second);
      size_t cnt = pred_gran_;
      while (cnt < data.size()) {
        rsb.AddKey(data[cnt - 1].first, data[cnt - 1].second);
        rsb.AddKey(data[cnt].first, data[cnt].second);
        cnt += pred_gran_;
      }
      if (cnt == data.size()) {
        rsb.AddKey(data[cnt - 1].first, data[cnt - 1].second);
      }
    } else {
      for (const auto& kv : data) {
        rsb.AddKey(kv.first, kv.second);
      }
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
    return "RS-PG-" + std::to_string(num_radix_bits_) + "_" +
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
  size_t pred_gran_;
};

#endif  // INDEXES_RS_DISK_PG_H_
