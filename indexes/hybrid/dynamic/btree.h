#ifndef INDEXES_HYBRID_DYNAMIC_BTREE_H_
#define INDEXES_HYBRID_DYNAMIC_BTREE_H_

#include <math.h>

#include <algorithm>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "./btree/btree_map.h"
#include "./dynamic_base.h"

template <typename K, typename V>
class BTreeIndex : public DynamicIndex<K, V> {
 public:
  struct param_t {};
  BTreeIndex(param_t) {}

  typedef K K_;
  typedef V V_;
  typedef std::pair<K_, V_> Record_;
  typedef std::vector<Record_> DataVev_;

  void Build(DataVev_& data) {
    btree_ = stx::btree_map<K, V>();
    btree_.bulk_load(data.begin(), data.end());
#ifdef PRINT_PROCESSING_INFO
    auto stat = btree_.get_stats();
    std::cout << "\nBtree use " << stat.innernodes << " inner nodes and "
              << stat.leaves << " leaf nodes for " << data.size() << " records"
              << ",\ttotal size:" << PRINT_MIB(GetTotalSize()) << " MiB"
              << std::endl;

    std::cout << "inner size:" << PRINT_MIB(stat.innernodes * 256.0)
              << std::endl;
#endif  // PRINT_PROCESSING_INFO
  }

  V Find(const K key) const {
    auto it = btree_.lower_bound(key);
    if (it == btree_.end() || it.key() != key) {
      return std::numeric_limits<V>::max();
    }
    return it.data();
  }

  V Scan(const K key, const int range) const {
    auto it = btree_.lower_bound(key);
    if (it == btree_.end() || it.key() != key) {
      return std::numeric_limits<V>::max();
    }
    V sum = it.data();
    for (int i = 0; i < range; i++) {
      it++;
      if (it == btree_.end()) {
        break;
      }
      sum += it.data();
    }
    return sum;
  }

  bool Insert(const K key, const V value) {
    auto it = btree_.find(key);
    if (it != btree_.end()) {
      btree_.erase(it);
    }
    btree_.insert(key, value);
    return true;
  }

  bool Update(const K key, const V value) {
    auto it = btree_.find(key);
    if (it == btree_.end()) {
      return false;
    }
    btree_.erase(it);
    btree_.insert(key, value);
    return true;
  }

  bool Delete(const K key) {
    btree_.erase(key);
    return true;
  }

  void Merge(DataVev_& merged_data, uint64_t num) {
    merged_data.resize(btree_.size() - num);

    uint64_t seg = btree_.size() / num;
    DataVev_ init_data(std::ceil(btree_.size() * 1.0 / seg));
    uint64_t init_cnt = 0, merge_cnt = 0, total_cnt = 0;

    auto it = btree_.begin();
    while (it != btree_.end()) {
      if (total_cnt % seg == 0) {
        init_data[init_cnt++] = {it.key(), it.data()};
      } else {
        merged_data[merge_cnt++] = {it.key(), it.data()};
      }
      it++;
      total_cnt++;
    }
    init_data.resize(init_cnt);
    merged_data.resize(merge_cnt);

    Build(init_data);
  }

  size_t size() const { return btree_.size(); }

  size_t GetNodeSize() const {
    auto stat = btree_.get_stats();
    return stat.innernodes * 256;
  }

  size_t GetTotalSize() const {
    auto stat = btree_.get_stats();
    return stat.innernodes * 256 + stat.leaves * 256;
  }

  void PrintEachPartSize() {
    auto stat = btree_.get_stats();
    std::cout << "\t\tbtree_ innernodes size:"
              << PRINT_MIB(stat.innernodes * 256)
              << ",\tbtree_ leaves size:" << PRINT_MIB(stat.leaves * 256)
              << ",\tin-memory data num:" << btree_.size()
              << ",\tin-memory MiB:"
              << PRINT_MIB(sizeof(Record_) * btree_.size())
              << ",\ttotal MiB:" << PRINT_MIB(GetTotalSize()) << std::endl;
  }

  std::string GetIndexName() const { return name_; }

  param_t GetIndexParams() const { return param_t(0); }

 private:
  std::string name_ = "BTree";
  stx::btree_map<K, V> btree_;
};

#endif  // INDEXES_HYBRID_DYNAMIC_BTREE_H_
