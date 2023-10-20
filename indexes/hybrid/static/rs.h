#ifndef INDEXES_HYBRID_STATIC_RS_H_
#define INDEXES_HYBRID_STATIC_RS_H_

#include "./rs/builder.h"
#include "./rs/radix_spline.h"
#include "./static_base.h"

template <typename K, typename V>
class RSIndex : public StaticIndex<K, V> {
 public:
  struct param_t {
    size_t num_radix_bits;
    size_t max_error;

    typename StaticIndex<K, V>::param_t disk_params;
  };

  RSIndex(param_t p = param_t(18, 1, {"/", 4096}))
      : StaticIndex<K, V>(p.disk_params) {
    num_radix_bits_ = p.num_radix_bits;
    max_error_ = p.max_error;
  }

  size_t GetStaticInitSize(typename StaticIndex<K, V>::DataVec_& data) const {
    rs::RadixSpline<K> rs;
    auto min = std::numeric_limits<K>::min();
    auto max = std::numeric_limits<K>::max();
    if (data.size() > 0) {
      min = data.front().first;
      max = data.back().first;
    }
    rs::Builder<K> rsb(min, max, num_radix_bits_, max_error_);
    for (const auto& kv : data) {
      rsb.AddKey(kv.first);
    }
    rs = rsb.Finalize();
    return rs.GetSize();
  }

  void Build(typename StaticIndex<K, V>::DataVec_& data) {
    // merge data
    typename StaticIndex<K, V>::DataVec_ train_data;
    StaticIndex<K, V>::MergeData(data, train_data);

    // rebuild the static index
    auto min = std::numeric_limits<K>::min();
    auto max = std::numeric_limits<K>::max();
    if (train_data.size() > 0) {
      min = train_data.front().first;
      max = train_data.back().first;
    }
    rs::Builder<K> rsb(min, max, num_radix_bits_, max_error_);
    for (const auto& kv : train_data) {
      rsb.AddKey(kv.first);
    }
    rs_ = rsb.Finalize();
#ifdef PRINT_PROCESSING_INFO
    std::cout << "\nRS use " << rs_.GetSegmentNum() << " models for "
              << train_data.size() << " records"
              << ",\t" << PRINT_MIB(GetNodeSize()) << " MiB" << std::endl;
#endif  // PRINT_PROCESSING_INFO
  }

  V Find(const K key) {
    // Already exclusive in the internal algorithm
    auto range = rs_.GetSearchBound(key);
    return StaticIndex<K, V>::FindData({range.begin, range.end}, key);
  }

  bool Update(const K key, const V value) {
    auto range = rs_.GetSearchBound(key);
    return StaticIndex<K, V>::UpdateData({range.begin, range.end}, key, value);
  }

  V Scan(const K key, const int length) {
    auto range = rs_.GetSearchBound(key);
    return StaticIndex<K, V>::ScanData({range.begin, range.end}, key, length);
  }

  size_t size() const { return StaticIndex<K, V>::size(); }

  size_t GetNodeSize() const { return rs_.GetSize(); }

  size_t GetTotalSize() const {
    return rs_.GetSize() + sizeof(typename StaticIndex<K, V>::Record_) * size();
  }

  void PrintEachPartSize() {
    std::cout << "\t\trs:" << PRINT_MIB(rs_.GetSize())
              << ",\ton-disk data num:" << size() << ",\ton-disk MiB:"
              << PRINT_MIB(sizeof(typename StaticIndex<K, V>::Record_) * size())
              << ",\ttotal MiB:" << PRINT_MIB(GetTotalSize()) << std::endl;
  }

  param_t GetIndexParams() const { return {num_radix_bits_, max_error_}; }

  std::string GetIndexName() const {
    return "RadixSpline-" + std::to_string(num_radix_bits_) + "_" +
           std::to_string(max_error_);
  }

 private:
  rs::RadixSpline<K> rs_;

  size_t num_radix_bits_;
  size_t max_error_;
};

#endif