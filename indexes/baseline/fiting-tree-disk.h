#ifndef INDEXES_BASELINE_FITING_TREE_DISK_H_
#define INDEXES_BASELINE_FITING_TREE_DISK_H_

#include "../../libraries/UpdatableLearnedIndexDisk/FITingTree/fiting_tree_memory.h"
#include "../base_index.h"

template <typename K, typename V>
class BaselineFitingTreeDisk : public BaseIndex<K, V> {
 public:
  struct param_t {
    std::string main_file;
    size_t max_error = 64;
  };

  BaselineFitingTreeDisk(param_t params = param_t(""))
      : error_(params.max_error),
        index_file_(params.main_file),
        ft_(params.max_error, const_cast<char *>(index_file_.c_str()), true,
            HYBRID_MODE),
        cnt_(0) {}

  void Build(typename BaseIndex<K, V>::DataVec_ &key_value) {
    std::vector<K> data2(key_value.size());
    fitingtree::Iterm *data = new fitingtree::Iterm[key_value.size()];
    for (size_t i = 0; i < key_value.size(); i++) {
      data2[i] = key_value[i].first;
      data[i].key = key_value[i].first;
      data[i].value = key_value[i].second;
    }
    ft_.bulk_load_pgm(data, key_value.size(), data2.begin(), data2.end(),
                      error_);
    std::cout << "inner node size:" << ft_.get_inner_size() << " bytes"
              << std::endl;
    std::cout << "file size:" << ft_.get_file_size() << " bytes" << std::endl;
  }

  V Find(const K key) {
    int c = 0;
    V res = ft_.lookup(key, &c);
    cnt_ += c;
    return res;
  }

  V Scan(const K key, const int range) {
    int c = 0;
    V res[range];
    ft_.scan(key, res, &c, range);
    cnt_ += c;
    V sum = 0;
    for (int i = 0; i < range; i++) {
      sum += res[i];
    }
    return sum;
  }

  bool Insert(const K key, const V value) {
    ft_.insert_key_entry_f(key, value);
    return true;
  }

  // TODO: update
  bool Update(const K key, const V value) { return Insert(key, value); }
  bool Delete(const K key) { return true; }

  size_t GetNodeSize() { return ft_.get_inner_size(); }

  size_t GetTotalSize() { return GetNodeSize() + ft_.get_file_size(); }

  void PrintEachPartSize() {
    std::cout << "\t\tft node size:" << PRINT_MIB(GetNodeSize())
              << ",\tft on-disk size:" << PRINT_MIB(ft_.get_file_size())
              << ",\ttotal MiB:" << PRINT_MIB(GetTotalSize()) << std::endl;
  }

  std::string GetIndexName() const { return name_; }

  param_t GetIndexParams() const { return param_t(0); }

 private:
  std::string name_ = "BASELINE_FITING_TREE_DISK";
  std::string index_file_;
  fitingtree::FITingTree ft_;
  size_t error_;

  int cnt_;
};

#endif  // !INDEXES_BASELINE_FITING_TREE_DISK_H_