#ifndef INDEXES_HYBRID_DYNAMIC_MULTI_THREADED_BTREE_H_
#define INDEXES_HYBRID_DYNAMIC_MULTI_THREADED_BTREE_H_

#include <math.h>

#include <algorithm>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "./btree/BTreeOLC.h"
#include "./dynamic_base.h"

template <typename K, typename V>
class MultiThreadedBTreeIndex : public DynamicIndex<K, V> {
 public:
  struct param_t {};
  MultiThreadedBTreeIndex(param_t) {}

  typedef K K_;
  typedef V V_;
  typedef std::pair<K_, V_> Record_;
  typedef std::vector<Record_> DataVev_;

  void Build(DataVev_& data) {
    btree_ = btreeolc::BTree<K_, V_>();
    for (size_t i = 0; i < data.size(); i++) {
      btree_.insert(data[i].first, data[i].second);
    }
#ifdef PRINT_PROCESSING_INFO
    std::cout << "\nBtree use " << btree_.inner_bytes() / 4096
              << " inner nodes and " << btree_.leaf_bytes() / 4096
              << " leaf nodes for " << data.size() << " records"
              << ",\ttotal size:" << PRINT_MIB(GetTotalSize()) << " MiB"
              << std::endl;

    std::cout << "inner size:" << PRINT_MIB(btree_.inner_bytes()) << std::endl;
#endif  // PRINT_PROCESSING_INFO
  }

  V Find(const K key) {
    V val;
    bool res = btree_.lookup(key, val);
    if (!res) {
      return std::numeric_limits<V>::max();
    }
    return val;
  }

  V Scan(const K key, const int range) {
    // TODO: scan
    return 0;
  }

  bool Insert(const K key, const V value) {
    btree_.insert(key, value);
    return true;
  }

  bool Update(const K key, const V value) {
    // TODO: update
    return false;
  }

  bool Delete(const K key) {
    // TODO: delete
    return false;
  }

  void Merge(DataVev_& merged_data) {
    merged_data.resize(btree_.size());
    btree_.MergeData(&merged_data);
  }

  size_t size() const { return btree_.size(); }

  size_t GetNodeSize() const { return btree_.inner_bytes(); }

  size_t GetTotalSize() const {
    return btree_.inner_bytes() + btree_.leaf_bytes();
  }

  void PrintEachPartSize() {
    std::cout << "\t\tbtree_ innernodes size:" << PRINT_MIB(GetNodeSize())
              << ",\tbtree_ leaves size:" << PRINT_MIB(btree_.leaf_bytes())
              << ",\tin-memory data num:" << btree_.size()
              << ",\tin-memory MiB:"
              << PRINT_MIB(sizeof(Record_) * btree_.size())
              << ",\ttotal MiB:" << PRINT_MIB(GetTotalSize()) << std::endl;
  }

  std::string GetIndexName() const { return name_; }

  param_t GetIndexParams() const { return param_t(0); }

 private:
  std::string name_ = "BTree";
  btreeolc::BTree<K, V> btree_;
};

#endif  // INDEXES_HYBRID_DYNAMIC_MULTI_THREADED_BTREE_H_
