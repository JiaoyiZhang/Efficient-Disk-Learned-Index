#ifndef INDEXES_HYBRID_DYNAMIC_PGM_H_
#define INDEXES_HYBRID_DYNAMIC_PGM_H_

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "../static/pgm/pgm_index_dynamic.hpp"
#include "./dynamic_base.h"

template <typename K, typename V>
class DynamicPGMIndex : public DynamicIndex<K, V> {
 public:
  struct param_t {};
  using PGMType = pgm::PGMIndex<K, 64, 16>;

  DynamicPGMIndex(param_t) {}

  typedef K K_;
  typedef V V_;
  typedef std::pair<K_, V_> Record_;
  typedef std::vector<Record_> DataVev_;

  void Build(DataVev_& data) {
    pgm_.bulk_load(data.begin(), data.end());
#ifdef PRINT_PROCESSING_INFO
    std::cout << "\nDynamic PGM use " << pgm_.segments_count() << " models for "
              << data.size() << " records"
              << ",\t" << PRINT_MIB(GetNodeSize()) << " MiB" << std::endl;
#endif  // PRINT_PROCESSING_INFO
  }

  V Find(const K key) const {
    auto it = pgm_.find(key);
    if (it == pgm_.end() || it->first != key) {
      return std::numeric_limits<V>::max();
    }
    return it->second;
  }

  V Scan(const K key, const int range) const {
    auto it = pgm_.find(key);
    if (it == pgm_.end() || it->first != key) {
      return std::numeric_limits<V>::max();
    }
    V sum = it->second;
    for (int i = 0; i < range; i++) {
      ++it;
      if (it == pgm_.end()) {
        break;
      }
      sum += it->second;
    }
    return sum;
  }

  bool Insert(const K key, const V value) {
    pgm_.insert_or_assign(key, value);
    return true;
  }

  bool Update(const K key, const V value) {
    pgm_.insert_or_assign(key, value);
    return true;
  }

  bool Delete(const K key) {
    pgm_.erase(key);
    return true;
  }

  void Merge(DataVev_& merged_data, uint64_t num) {
    merged_data.resize(pgm_.size() - num);
    DataVev_ init_data(num);

    uint64_t seg = pgm_.size() / num;
    uint64_t init_cnt = 0, merge_cnt = 0, total_cnt = 0;

    auto it = pgm_.begin();
    while (it != pgm_.end()) {
      if (total_cnt % seg == 0) {
        init_data[init_cnt++] = {it->first, it->second};
      } else {
        merged_data[merge_cnt++] = {it->first, it->second};
      }
      ++it;
      total_cnt++;
    }
    init_data.resize(init_cnt);
    merged_data.resize(merge_cnt);

    Build(init_data);
  }

  size_t size() const { return pgm_.size(); }

  size_t GetNodeSize() const { return pgm_.index_size_in_bytes(); }

  size_t GetTotalSize() const { return pgm_.size_in_bytes(); }

  void PrintEachPartSize() {
    std::cout << "\t\tpgm_ node size:" << PRINT_MIB(pgm_.index_size_in_bytes())
              << ",\tpgm data size:" << PRINT_MIB(pgm_.size_in_bytes())
              << ",\tin-memory data num:" << size()
              << ",\tin-memory MiB:" << PRINT_MIB(sizeof(Record_) * size())
              << ",\ttotal MiB:" << PRINT_MIB(GetTotalSize()) << std::endl;
  }

  std::string GetIndexName() const { return "DynamicPGM-64"; }

  param_t GetIndexParams() const { return param_t(0); }

 private:
  pgm::DynamicPGMIndex<K, V, PGMType> pgm_;
};

#endif  // INDEXES_HYBRID_DYNAMIC_PGM_H_
