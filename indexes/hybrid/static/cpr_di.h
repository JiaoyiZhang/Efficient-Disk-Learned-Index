#ifndef INDEXES_HYBRID_STATIC_CPR_DI_H_
#define INDEXES_HYBRID_STATIC_CPR_DI_H_

#define HYBRID_BENCHMARK
#include "../../Compressed-Disk-Oriented-Index/di_v4.h"
#include "./static_base.h"

template <typename K, typename V>
class StaticCprDI : public StaticIndex<K, V> {
 public:
  struct param_t {
    float lambda;
    size_t record_per_page;

    typename StaticIndex<K, V>::param_t disk_params;
  };

  StaticCprDI(param_t p) : StaticIndex<K, V>(p.disk_params) {
    lambda_ = p.lambda;
    record_per_page_ = p.record_per_page;
  }

  size_t GetStaticInitSize(typename StaticIndex<K, V>::DataVec_& data) const {
    compressed_disk_index::DiskOrientedIndexV4<K, V> di(record_per_page_);
    di.Build(data, lambda_);
    return di.GetSize();
  }

  void Build(typename StaticIndex<K, V>::DataVec_& data) {
    // merge data
    typename StaticIndex<K, V>::DataVec_ train_data;
    StaticIndex<K, V>::MergeData(data, train_data);

    // rebuild the static index
    di_ = compressed_disk_index::DiskOrientedIndexV4<K, V>(record_per_page_);
    di_.Build(train_data, lambda_);

#ifdef PRINT_PROCESSING_INFO
    std::cout << "\nCompressed DI use " << di_.GetModelNum() << " models for "
              << train_data.size() << " records"
              << ",\t" << PRINT_MIB(GetNodeSize()) << " MiB" << std::endl;
#endif  // PRINT_PROCESSING_INFO
  }

  V Find(const K key) {
    auto range = di_.GetSearchBound(key);
    return StaticIndex<K, V>::FindData({range.begin, range.end}, key);
  }

  V Scan(const K key, const int length) {
    auto range = di_.GetSearchBound(key);
    return StaticIndex<K, V>::ScanData({range.begin, range.end}, key, length);
  }

  bool Update(const K key, const V value) {
    auto range = di_.GetSearchBound(key);
    return StaticIndex<K, V>::UpdateData({range.begin, range.end}, key, value);
  }

  size_t size() const { return StaticIndex<K, V>::size(); }

  size_t GetNodeSize() const { return di_.GetSize(); }

  size_t GetTotalSize() const {
    return GetNodeSize() + sizeof(typename StaticIndex<K, V>::Record_) * size();
  }

  void PrintEachPartSize() {
    std::cout << "\t\tdi:" << PRINT_MIB(GetNodeSize())
              << ",\ton-disk data num:" << size() << ",\ton-disk MiB:"
              << PRINT_MIB(sizeof(typename StaticIndex<K, V>::Record_) * size())
              << ",\ttotal MiB:" << PRINT_MIB(GetTotalSize()) << std::endl;
  }

  param_t GetIndexParams() const { return lambda_; }

  std::string GetIndexName() const {
    auto str0 = std::to_string(lambda_);
    return "StaticCprDI-" + str0.substr(0, str0.find(".") + 3);
  }

 private:
  compressed_disk_index::DiskOrientedIndexV4<K, V> di_;

  float lambda_;
  size_t record_per_page_;
};

#endif