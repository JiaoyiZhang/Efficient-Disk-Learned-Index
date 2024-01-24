#ifndef INDEXES_HYBRID_STATIC_MULTI_THREADED_CPR_DI_H_
#define INDEXES_HYBRID_STATIC_MULTI_THREADED_CPR_DI_H_

#define HYBRID_BENCHMARK
#include "../../Compressed-Disk-Oriented-Index/di_v4.h"
#include "./static_base.h"

template <typename K, typename V>
class MultiThreadedStaticCprDI : public MultiThreadedStaticIndex<K, V> {
 public:
  typedef MultiThreadedStaticIndex<K, V> Base;
  typedef compressed_disk_index::DiskOrientedIndexV4<K, V> IndexType;
  struct param_t {
    float lambda;
    size_t record_per_page;

    typename Base::param_t disk_params;
  };

  MultiThreadedStaticCprDI(param_t p) : Base(p.disk_params) {
#ifdef PRINT_PROCESSING_INFO
    std::cout << "lambda:" << p.lambda
              << ",\trecord_per_page:" << p.record_per_page << std::endl;
#endif
    lambda_ = p.lambda;
    record_per_page_ = p.record_per_page;
  }

  MultiThreadedStaticCprDI& operator=(const MultiThreadedStaticCprDI& other) {
    Base::operator=(other);
    di_ = std::vector<IndexType>(Base::merge_thread_num_);
    for (size_t i = 0; i < Base::merge_thread_num_; i++) {
      di_[i] = other.di_[i];
    }
    total_index_size_.store(other.total_index_size_.load());
    lambda_ = other.lambda_;
    record_per_page_ = other.record_per_page_;

    return *this;
  }

  size_t GetStaticInitSize(typename Base::DataVec_& data) const {
    IndexType di(record_per_page_);
    di.Build(data, lambda_);
    return di.GetSize();
  }

  void Build(typename Base::DataVec_& data, int thread_id) {
    // merge data
    Base::Init(data, thread_id);

    // rebuild the static index
    IndexType tmp(record_per_page_);
    di_ = std::vector<IndexType>(Base::merge_thread_num_);
    int s = 0, e = 0;
    int sub_item_num = std::ceil(data.size() * 1.0 / Base::merge_thread_num_);
    total_index_size_ = 0;
    for (int i = 0; i < Base::merge_thread_num_; i++) {
      di_[i] = tmp;
      s = i * sub_item_num;
      e = std::min((i + 1) * sub_item_num, static_cast<int>(data.size()));
      typename Base::DataVec_ train_data(data.begin() + s, data.begin() + e);
      for (size_t j = 0; j < train_data.size(); j++) {
        train_data[j].second = j;
      }
      di_[i].Build(train_data, lambda_);
      total_index_size_.fetch_add(di_[i].GetSize());

#ifdef PRINT_PROCESSING_INFO
      std::cout << "\nCompressed DI use " << di_[i].GetModelNum()
                << " models for " << train_data.size() << " records"
                << ",\t" << PRINT_MIB(GetNodeSize()) << " MiB" << std::endl;
#endif  // PRINT_PROCESSING_INFO
    }
  }

  void Merge(typename Base::DataVec_& data, int thread_id) {
    Base::PartitionData(data, thread_id);
    total_index_size_ = 0;
    auto pid = Base::ObtainMergeTask(thread_id);
    if (pid >= 0) {
      MergeSubData(data, thread_id, pid);
    }
  }

  void MergeSubData(typename Base::DataVec_& data, int thread_id,
                    int partition_id) {
    // merge data
    typename Base::DataVec_ train_data;
    Base::MergeSubData(data, train_data, thread_id, partition_id);

#ifdef BREAKDOWN
    auto start = std::chrono::high_resolution_clock::now();
#endif
    // rebuild the static index
    di_[partition_id] = IndexType(record_per_page_);
    di_[partition_id].Build(train_data, lambda_);
#ifdef BREAKDOWN
    auto end = std::chrono::high_resolution_clock::now();
    Base::model_training_lat[thread_id] +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
#endif

    total_index_size_.fetch_add(di_[partition_id].GetSize());
#ifdef PRINT_PROCESSING_INFO
    std::cout << "\nCompressed DI use " << di_[partition_id].GetModelNum()
              << " models for " << train_data.size() << " records"
              << ",\t" << PRINT_MIB(total_index_size_) << " MiB" << std::endl;
#endif  // PRINT_PROCESSING_INFO
    Base::FinishMerge(thread_id);
  }

  V Find(const K key, int thread_id) {
    auto pid = Base::GetPartitionID(key);
    auto range = di_[pid].GetSearchBound(key);
    SearchRange static_range = {range.begin, range.end};
    return Base::FindData(static_range, key, thread_id, pid);
  }

  V Scan(const K key, const int length, int thread_id) {
    auto pid = Base::GetPartitionID(key);
    auto range = di_[pid].GetSearchBound(key);
    SearchRange static_range = {range.begin, range.end};
    return Base::ScanData(static_range, key, length, thread_id, pid);
  }

  bool Update(const K key, const V value, int thread_id) {
    auto pid = Base::GetPartitionID(key);
    auto range = di_[pid].GetSearchBound(key);
    SearchRange static_range = {range.begin, range.end};
    return Base::UpdateData(static_range, key, value, thread_id, pid);
  }

#ifdef BREAKDOWN
  void PrintBreakdown() {
    Base::PrintBreakdown();
    std::cout << "***********DI BREAKDOWN***************" << std::endl;
    for (int i = 0; i < di_.size(); i++) {
      di_[i].PrintBreakdown();
    }
  }
#endif

  inline size_t size() const { return Base::size(); }

  inline size_t GetNodeSize() const { return total_index_size_.load(); }

  inline size_t GetTotalSize() const {
    return GetNodeSize() + Base::GetDiskBytes();
  }

  void PrintEachPartSize() {
    std::cout << "\t\tdi:" << PRINT_MIB(GetNodeSize())
              << ",\ton-disk data num:" << size()
              << ",\ton-disk MiB:" << PRINT_MIB(Base::GetDiskBytes())
              << ",\ttotal MiB:" << PRINT_MIB(GetTotalSize()) << std::endl;
  }

  param_t GetIndexParams() const { return lambda_; }

  std::string GetIndexName() const {
    auto str0 = std::to_string(lambda_);
    return "MultiThreadedStaticCprDI-" + str0.substr(0, str0.find(".") + 3);
  }

 private:
  // IndexType di_;
  std::vector<IndexType> di_;

  std::atomic<size_t> total_index_size_;

  float lambda_;
  size_t record_per_page_;
};

#endif