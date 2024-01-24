#ifndef INDEXES_BTREE_MT_DISK_H_
#define INDEXES_BTREE_MT_DISK_H_

#include "../../libraries/UpdatableLearnedIndexDisk/B+Tree/mt_b_tree.h"
#include "../base_index.h"

template <typename K, typename V>
class BaselineBTreeMTDisk : public MultiThreadedBaseIndex<K, V> {
 public:
  struct param_t {
    std::string main_file;
  };

  BaselineBTreeMTDisk(param_t params = param_t(""))
      : index_file_(params.main_file), btree_(index_file_.c_str()) {}

  void Build(typename BaseIndex<K, V>::DataVec_& key_value) {
    auto seed = std::chrono::system_clock::now().time_since_epoch().count();
    auto dataset = key_value;
    std::shuffle(dataset.begin(), dataset.end(),
                 std::default_random_engine(seed));
#pragma omp parallel for num_threads(64)
    for (int i = 0; i < dataset.size(); i++) {
      btree_.insert(dataset[i].first, dataset[i].second);
    }

    btree_.direct_open(index_file_);
  }

  V Find(const K key, int thread_id) {
    V res = btree_.lookup(key);
    return res;
  }

  V Scan(const K key, const int range, int thread_id) {
    V sum = 0;
    return sum;
  }

  bool Insert(const K key, const V value, int thread_id) {
    btree_.insert(key, value);
    return true;
  }

  bool Update(const K key, const V value, int thread_id) { return true; }

  bool Delete(const K key, int thread_id) { return true; }

  void FreeBuffer(){};

#ifdef BREAKDOWN
  void PrintBreakdown(){};
#endif

  size_t GetNodeSize() { return 0; }

  size_t GetTotalSize() { return GetNodeSize() + 0; }

  void PrintEachPartSize() {
    std::cout << "\t\tbtree node size:" << PRINT_MIB(GetNodeSize())
              << ",\tbtree on-disk size:" << PRINT_MIB(0)
              << ",\ttotal MiB:" << PRINT_MIB(GetTotalSize()) << std::endl;
  }

  std::string GetIndexName() const { return name_; }

  param_t GetIndexParams() const { return param_t(0); }

 private:
  std::string name_ = "BASELINE_BTree_MT_Disk";
  std::string index_file_;
  ThreadSafeBTreeDisk<K, V> btree_;
};

#endif  // !INDEXES_BTREE_MT_DISK_H_