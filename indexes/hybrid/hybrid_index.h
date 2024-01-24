#ifndef INDEXES_HYBRID_INDEX_H_
#define INDEXES_HYBRID_INDEX_H_

#include <assert.h>

#include "../base_index.h"

#define INIT_SIZE 100

template <typename K, typename V, typename DynamicType, typename StaticType>
class HybridIndex : public BaseIndex<K, V> {
 public:
  struct param_t {
    typename DynamicType::param_t d_params_;
    typename StaticType::param_t s_params_;
    size_t memory_budget_;
  };

  HybridIndex(param_t params)
      : dynamic_index_(params.d_params_),
        static_index_(params.s_params_),
        merge_cnt_(0),
        mem_find_cnt_(0),
        disk_find_cnt_(0),
        mem_update_cnt_(0),
        disk_update_cnt_(0),
        mem_insert_cnt_(0),
        max_dynamic_usage_(0),
        max_dynamic_index_usage_(0),
        max_memory_usage_(0),
        max_buffer_size_(0),
        memory_budget_(params.memory_budget_),
        dynamic_budget_(0) {}

  typedef typename BaseIndex<K, V>::DataVec_ BaseVec;
  void Build(BaseVec& data) {
    // collect some data for learned indexes to train models
    size_t init_size = INIT_SIZE;
    BaseVec dynamic_data(init_size);
    BaseVec static_data(data.size());
    assert(static_data.size() > init_size);
    uint64_t seg = static_data.size() / init_size;
    uint64_t dynamic_cnt = 0, static_cnt = 0, total_cnt = 0;

    auto it = data.begin();
    while (it != data.end()) {
      if (total_cnt % seg == 0 && dynamic_cnt < init_size) {
        dynamic_data[dynamic_cnt++] = {it->first, it->second};
      } else {
        static_data[static_cnt++] = {it->first, it->second};
      }
      it++;
      total_cnt++;
    }
    dynamic_data.resize(dynamic_cnt);
    static_data.resize(static_cnt);
#ifdef PRINT_PROCESSING_INFO
    std::cout << "#init_records in dynamic_index_:" << dynamic_data.size()
              << std::endl;
    std::cout << "#init_records in static_index_:" << static_data.size()
              << std::endl;
#endif

    dynamic_index_.Build(dynamic_data);
    static_index_.Build(static_data);

    // get the remaining memory budget for the dynamic index
    size_t static_memory = static_index_.GetNodeSize();
    std::cout << "memory_budget:" << PRINT_MIB(memory_budget_)
              << " MiB,\tstatic_memory:" << PRINT_MIB(static_memory) << " MiB"
              << std::endl;
    if (memory_budget_ <= static_memory) {
      throw std::runtime_error("Need more memory budget!");
    }
    dynamic_budget_ = memory_budget_ - static_memory;
    std::cout << "\tdynamic_budget_:" << PRINT_MIB(dynamic_budget_)
              << std::endl;

    max_memory_usage_ = GetCurrMemoryUsage();
    max_dynamic_usage_ = dynamic_index_.GetTotalSize();
    max_dynamic_index_usage_ = dynamic_index_.GetNodeSize();
#ifdef CHECK_CORRECTION
    for (int i = 0; i < dynamic_cnt; i++) {
      auto res = dynamic_index_.Find(dynamic_data[i].first);
      if (dynamic_data[i].second != res) {
        std::cout << "find dynamic " << i << ",\tk:" << dynamic_data[i].first
                  << ",\tv:" << dynamic_data[i].second << ",\tres:" << res
                  << std::endl;
      }
    }
    std::cout << "Check dynamic index over! " << std::endl;
#endif
  }

  V Find(const K key) {
    // lookup in the dynamic index
    V res = dynamic_index_.Find(key);
    mem_find_cnt_++;
    if (res == std::numeric_limits<V>::max()) {
      // lookup in the static index
      res = static_index_.Find(key);
      disk_find_cnt_++;
    }
    return res;
  }

  V Scan(const K key, const int range) {
    V res = dynamic_index_.Scan(key, range);
    mem_find_cnt_++;
    disk_find_cnt_++;
    if (res == std::numeric_limits<V>::max()) {
      res = static_index_.Scan(key, range);
    } else {
      res += static_index_.Scan(key, range);
    }
    return res;
  }

  bool Insert(const K key, const V value) {
    mem_insert_cnt_++;
    if (dynamic_index_.GetTotalSize() > dynamic_budget_) {
#ifdef PRINT_PROCESSING_INFO
      auto static_size = static_index_.size();
      std::cout << "need to merge! dynamic_size:"
                << dynamic_index_.GetTotalSize()
                << ",\tstatic_size:" << static_size
                << ",\tmax_buffer_size_:" << max_buffer_size_ << std::endl;
#endif
      merge_cnt_++;
      Merge();
      dynamic_budget_ = memory_budget_ - static_index_.GetNodeSize();
    }
#ifdef BREAKDOWN
    auto start = std::chrono::high_resolution_clock::now();
#endif
    auto res = dynamic_index_.Insert(key, value);
#ifdef BREAKDOWN
    auto end = std::chrono::high_resolution_clock::now();
    dynamic_insert_lat +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
#endif
#ifdef CHECK_CORRECTION
    V new_val = dynamic_index_.Find(key);
    if (new_val != value) {
      std::cout << "insert wrong! key:" << key << ",\tval:" << value
                << ",\tnew_val:" << new_val << std::endl;
      dynamic_index_.Find(key);
    }
#endif
    return res;
  }

  bool Update(const K key, const V value) {
    // update in the dynamic index
    bool success = dynamic_index_.Update(key, value);
    if (success) {
      mem_update_cnt_++;
#ifdef CHECK_CORRECTION
      V new_val = dynamic_index_.Find(key);
      if (new_val != value) {
        std::cout << "dynamic update wrong! key:" << key << ",\tval:" << value
                  << ",\tnew_val:" << new_val << std::endl;
        dynamic_index_.Find(key);
      }
#endif
    } else {
      // update in the static index
      success = static_index_.Update(key, value);
      disk_update_cnt_++;
#ifdef CHECK_CORRECTION
      V new_val = static_index_.Find(key);
      if (new_val != value) {
        std::cout << "static update wrong! key:" << key << ",\tval:" << value
                  << ",\tnew_val:" << new_val << std::endl;
        static_index_.Find(key);
      }
#endif
    }
    return success;
  }

  bool Delete(const K key) {
    dynamic_index_.Delete(key);
    // static_index_.Delete(key);
    return true;
  }

  size_t GetCurrMemoryUsage() const {
    return dynamic_index_.GetTotalSize() + static_index_.GetNodeSize();
  }
  size_t GetNodeSize() const {
    // return dynamic_index_.GetTotalSize() + static_index_.GetNodeSize();
    return max_memory_usage_;
  }
  size_t GetTotalSize() const {
    return dynamic_index_.GetTotalSize() + static_index_.GetTotalSize();
  }
  void PrintEachPartSize() {
    max_memory_usage_ = std::max(max_memory_usage_, GetCurrMemoryUsage());
    max_dynamic_usage_ =
        std::max(max_dynamic_usage_, dynamic_index_.GetTotalSize());
    max_dynamic_index_usage_ =
        std::max(max_dynamic_index_usage_, dynamic_index_.GetNodeSize());
    max_buffer_size_ = std::max(max_buffer_size_, dynamic_index_.size());
    std::cout << "-------------dynamic info-------------" << std::endl;
    dynamic_index_.PrintEachPartSize();
    std::cout << "-------------static info---------------" << std::endl;
    static_index_.PrintEachPartSize();
    std::cout << "-------------processing info-------------" << std::endl;
    std::cout << "\t\tmerge cnt:" << merge_cnt_
              << ",\tin-memory find cnt:" << mem_find_cnt_
              << ",\ton-disk find cnt:" << disk_find_cnt_
              << ",\tin-memory insert:" << mem_insert_cnt_ << std::endl;
    std::cout << "-------------memory usage---------------" << std::endl;
    std::cout << "\tmemory_budget:" << PRINT_MIB(memory_budget_)
              << " MiB,\tdynamic_budget:" << PRINT_MIB(dynamic_budget_)
              << " MiB,\tmax_buffer_size:" << max_buffer_size_
              << ",\tmax_buffer_usage_:"
              << PRINT_MIB(max_buffer_size_ * sizeof(std::pair<K, V>)) << " MiB"
              << std::endl;
    std::cout << "\tmax_dynamic_usage_:" << PRINT_MIB(max_dynamic_usage_)
              << " MiB,\tmax_dynamic_index_usage_:"
              << PRINT_MIB(max_dynamic_index_usage_)
              << " MiB,\tmax_dynamic_data_node_usage:"
              << PRINT_MIB(max_dynamic_usage_ - max_dynamic_index_usage_)
              << " MiB,\tmax_static_usage_:"
              << PRINT_MIB(static_index_.GetNodeSize())
              << " MiB,\tmax_memory_usage_:" << PRINT_MIB(max_memory_usage_)
              << " MiB" << std::endl;
    std::cout << "-------------print over---------------" << std::endl;
#ifdef BREAKDOWN
    if (merge_cnt_ > 0) {
      std::cout << "dynamic insert avg latency:"
                << dynamic_insert_lat / mem_insert_cnt_ / 1e6 << " ms"
                << std::endl;
      std::cout << "dynamic merge avg latency:"
                << dynamic_merge_lat / merge_cnt_ / 1e6 << " ms" << std::endl;
      std::cout << "static merge avg latency:"
                << static_merge_lat / merge_cnt_ / 1e6 << " ms" << std::endl;
      std::cout << "-------------print over---------------" << std::endl;
    }
#endif
  }
  std::string GetIndexName() const {
    return GetDynamicName() + "_" + GetStaticName();
  }
  typename DynamicType::param_t GetDynamicParams() const {
    return dynamic_index_.GetIndexParams();
  }
  typename StaticType::param_t GetStaticParams() const {
    return static_index_.GetIndexParams();
  }

 private:
  void Merge() {
    max_memory_usage_ = std::max(max_memory_usage_, GetCurrMemoryUsage());
    max_dynamic_usage_ =
        std::max(max_dynamic_usage_, dynamic_index_.GetTotalSize());
    max_dynamic_index_usage_ =
        std::max(max_dynamic_index_usage_, dynamic_index_.GetNodeSize());
    max_buffer_size_ = std::max(max_buffer_size_, dynamic_index_.size());
    BaseVec dynamic_data;

#ifdef BREAKDOWN
    auto start = std::chrono::high_resolution_clock::now();
#endif
    dynamic_index_.Merge(dynamic_data, INIT_SIZE);
#ifdef BREAKDOWN
    auto end = std::chrono::high_resolution_clock::now();
    dynamic_merge_lat +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
    start = std::chrono::high_resolution_clock::now();
#endif

    static_index_.Build(dynamic_data);
#ifdef BREAKDOWN
    end = std::chrono::high_resolution_clock::now();
    static_merge_lat +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
#endif
  }

  std::string GetDynamicName() const { return dynamic_index_.GetIndexName(); }
  std::string GetStaticName() const { return static_index_.GetIndexName(); }

  DynamicType dynamic_index_;
  StaticType static_index_;
#ifdef BREAKDOWN
  double dynamic_merge_lat = 0.0;
  double static_merge_lat = 0.0;
  double dynamic_insert_lat = 0.0;
#endif

  size_t merge_cnt_;
  size_t mem_find_cnt_;
  size_t disk_find_cnt_;
  size_t mem_update_cnt_;
  size_t disk_update_cnt_;
  size_t mem_insert_cnt_;

  size_t max_dynamic_usage_;
  size_t max_dynamic_index_usage_;
  size_t max_memory_usage_;
  size_t max_buffer_size_;

  size_t memory_budget_;
  size_t dynamic_budget_;
};

#endif  // !INDEXES_HYBRID_INDEX_H_