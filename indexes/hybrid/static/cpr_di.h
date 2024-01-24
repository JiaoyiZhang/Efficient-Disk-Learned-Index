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
    total_index_size_ = 0;
    disk_size_ = 0;
  }

  size_t GetStaticInitSize(typename StaticIndex<K, V>::DataVec_& data) const {
    compressed_disk_index::DiskOrientedIndexV4<K, V> di(record_per_page_);
    di.Build(data, lambda_);
    return di.GetSize();
  }

  void Build(typename StaticIndex<K, V>::DataVec_& data) {
    // merge data
#ifdef BREAKDOWN
    auto start = std::chrono::high_resolution_clock::now();
#endif

    typename StaticIndex<K, V>::DataVec_ train_data;
    StaticIndex<K, V>::MergeData(data, train_data);
    for (size_t j = 0; j < train_data.size(); j++) {
      train_data[j].second = j;
    }

#ifdef BREAKDOWN
    auto end = std::chrono::high_resolution_clock::now();
    if (merge_cnt >= 0) {
      merge_lat +=
          std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
              .count();
    }
#endif

    // rebuild the static index
    di_ = compressed_disk_index::DiskOrientedIndexV4<K, V>(record_per_page_);
#ifdef BREAKDOWN
    start = std::chrono::high_resolution_clock::now();
#endif
    di_.Build(train_data, lambda_);
#ifdef BREAKDOWN
    end = std::chrono::high_resolution_clock::now();
    if (merge_cnt >= 0) {
      train_lat +=
          std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
              .count();
    }
    merge_cnt++;
#endif
    total_index_size_ = di_.GetSize();
    disk_size_ = sizeof(typename StaticIndex<K, V>::Record_) * size();
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

  inline size_t size() const { return StaticIndex<K, V>::size(); }

  inline size_t GetNodeSize() const { return total_index_size_; }

  inline size_t GetTotalSize() const { return total_index_size_ + disk_size_; }

  void PrintEachPartSize() {
    std::cout << "\t\tdi:" << PRINT_MIB(GetNodeSize())
              << ",\ton-disk data num:" << size() << ",\ton-disk MiB:"
              << PRINT_MIB(sizeof(typename StaticIndex<K, V>::Record_) * size())
              << ",\ttotal MiB:" << PRINT_MIB(GetTotalSize()) << std::endl;
#ifdef BREAKDOWN
    if (merge_cnt > 0) {
      std::cout << "merge cnt:" << merge_cnt << std::endl;
      std::cout << " merge avg latency:" << merge_lat / merge_cnt / 1e6 << " ms"
                << std::endl;
      std::cout << " train avg latency:" << train_lat / merge_cnt / 1e6 << " ms"
                << std::endl;
      std::cout << "-------------print over---------------" << std::endl;
    }
    StaticIndex<K, V>::Breakdown();
#endif
  }

  param_t GetIndexParams() const { return lambda_; }

  std::string GetIndexName() const {
    auto str0 = std::to_string(lambda_);
    return "StaticCprDI-" + str0.substr(0, str0.find(".") + 3);
  }

 private:
  compressed_disk_index::DiskOrientedIndexV4<K, V> di_;

#ifdef BREAKDOWN
  double merge_lat = 0.0;
  double train_lat = 0.0;
  int merge_cnt = -1;
#endif

  float lambda_;
  size_t record_per_page_;
  size_t total_index_size_;
  size_t disk_size_;
};

#endif