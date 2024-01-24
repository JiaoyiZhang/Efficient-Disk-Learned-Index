#ifndef INDEXES_BASELINE_PGM_DISK_H_
#define INDEXES_BASELINE_PGM_DISK_H_

#include "../../libraries/UpdatableLearnedIndexDisk/PGM/pgm/pgm_index_dynamic.hpp"
#include "../base_index.h"

template <typename K, typename V>
class BaselinePGMDisk : public BaseIndex<K, V> {
 public:
  struct param_t {
    std::string main_file;
    size_t memory_budget_;
  };

  typedef pgm_baseline_disk::DynamicPGMIndex<K, V> PGM_DISK;

  BaselinePGMDisk(param_t params = param_t(""))
      : index_file_(params.main_file),
        memory_budget_(params.memory_budget_),
        cnt_(0) {}

  void Build(typename BaseIndex<K, V>::DataVec_ &key_value) {
    max_key_ = key_value.back().first;
    bool pgm_inner_on_disk = true;
    if (HYBRID_MODE == 1) {
      pgm_inner_on_disk = false;
    }
    pgm_.build(memory_budget_, const_cast<char *>(index_file_.c_str()), true,
               pgm_inner_on_disk, key_value.begin(), key_value.end());
    std::cout << "inner node size:" << pgm_.report_main_memory_size()
              << " bytes" << std::endl;
    std::cout << "file size:" << pgm_.report_disk_file_size() << " bytes"
              << std::endl;
  }

  V Find(const K key) {
    int c = 0;
    V res = 0;
    int ic = 0;
    int lc = 0;
    res = pgm_.find_on_disk(key, &c, &ic, &lc);
    cnt_ += c;
    return res;
  }

  V Scan(const K key, const int range) {
    int c = 0;
    typename PGM_DISK::ItemOnDisk *a = new typename PGM_DISK::ItemOnDisk[102];
    typename PGM_DISK::ItemOnDisk *b = new typename PGM_DISK::ItemOnDisk[102];
    int fsize = 0;
    auto use_b = pgm_.range_on_disk(key, max_key_, range, &c, a, b, &fsize);
    cnt_ += c;
    V sum = 0;
    for (int i = 0; i < range; i++) {
      if (use_b) {
        sum += b[i].value;
      } else {
        sum += a[i].value;
      }
    }
    return sum;
  }

  bool Insert(const K key, const V value) {
    long long sel, inl, sml;
    int smo_c = 0;
    int update_t = 0;
    pgm_.insert_on_disk(key, value, &sel, &inl, &sml, &smo_c, &update_t);
    return true;
  }

  bool Update(const K key, const V value) {
    int c = 0;
    int ic = 0;
    int lc = 0;
    auto succ = pgm_.update_on_disk(key, value, &c, &ic, &lc);
    cnt_ += c;
    return succ;
  }
  bool Delete(const K key) { return true; }

  size_t GetNodeSize() { return pgm_.report_main_memory_size(); }

  size_t GetTotalSize() { return GetNodeSize() + pgm_.report_disk_file_size(); }

  void PrintEachPartSize() {
    std::cout << "\t\tpgm node size:" << PRINT_MIB(GetNodeSize())
              << ",\tpgm on-disk size:"
              << PRINT_MIB(pgm_.report_disk_file_size())
              << ",\ttotal MiB:" << PRINT_MIB(GetTotalSize()) << std::endl;
  }

  std::string GetIndexName() const { return name_; }

  param_t GetIndexParams() const { return param_t(0); }

 private:
  std::string name_ = "BASELINE_OPTIMIZED_PGM";
  std::string index_file_;
  size_t memory_budget_;
  PGM_DISK pgm_;
  K max_key_;

  int cnt_;
};

#endif  // !INDEXES_BASELINE_PGM_DISK_H_