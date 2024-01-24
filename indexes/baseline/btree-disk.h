#ifndef INDEXES_BTREE_DISK_H_
#define INDEXES_BTREE_DISK_H_

#include "../../libraries/UpdatableLearnedIndexDisk/B+Tree/b_tree.h"
#include "../base_index.h"

template <typename K, typename V>
class BaselineBTreeDisk : public BaseIndex<K, V> {
 public:
  struct param_t {
    size_t inner_on_disk_num;
    std::string main_file;
  };

  BaselineBTreeDisk(param_t params = param_t(0, ""))
      : index_file_(params.main_file),
        btree_(HYBRID_MODE, true, const_cast<char*>(index_file_.c_str()), true),
        // btree_(HYBRID_MODE, true, const_cast<char*>(index_file_.c_str()),
        //        false),
        cnt_(0),
        inner_on_disk_num_(params.inner_on_disk_num) {}

  void Build(typename BaseIndex<K, V>::DataVec_& key_value) {
    typename BaseIndex<K, V>::DataVec_ dataset(key_value.begin(),
                                               key_value.end());
    LeafNodeIterm* data = new LeafNodeIterm[key_value.size()];
    for (int i = 0; i < key_value.size(); i++) {
      data[i].key = key_value[i].first;
      data[i].value = key_value[i].second;
    }
    btree_.bulk_load(data, key_value.size(), 0.7, inner_on_disk_num_);
    btree_.sync_metanode();

    // auto seed = std::chrono::system_clock::now().time_since_epoch().count();
    // std::shuffle(dataset.begin(), dataset.end(),
    //              std::default_random_engine(seed));
    // for (auto kv : dataset) {
    //   btree_.insert_key_entry(kv.first, kv.second);
    // }
  }

  V Find(const K key) {
    int c = 0;
    V res = 0;
    bool found = btree_.lookup(key, &c, &res);
#ifdef CHECK_CORRECTION
    if (!found) {
      std::cout << "btree baseline find key " << key << " wrong! res:" << res
                << std::endl;
      btree_.lookup(key, &c, &res);
    }
#endif
    cnt_ += c;
    return res;
  }

  V Scan(const K key, const int range) {
    int c = 0;
    V res[range];
    btree_.scan(res, key, range, &c);
    cnt_ += c;
    V sum = 0;
    for (int i = 0; i < range; i++) {
      sum += res[i];
    }
    return sum;
  }

  bool Insert(const K key, const V value) {
    btree_.insert_key_entry(key, value);
    return true;
  }

  bool Update(const K key, const V value) {
    int c = 0;
    bool res = btree_.update(key, value, &c);
    cnt_ += c;
    return res;
  }

  bool Delete(const K key) {
    // btree_.erase(key);
    return true;
  }

  size_t GetNodeSize() { return btree_.get_inner_size(); }

  size_t GetTotalSize() { return GetNodeSize() + btree_.get_file_size(); }

  void PrintEachPartSize() {
    std::cout << "\t\tbtree node size:" << PRINT_MIB(GetNodeSize())
              << ",\tbtree on-disk size:" << PRINT_MIB(btree_.get_file_size())
              << ",\ttotal MiB:" << PRINT_MIB(GetTotalSize()) << std::endl;
  }

  std::string GetIndexName() const {
    return name_ + std::to_string(inner_on_disk_num_);
  }

  param_t GetIndexParams() const { return param_t(0); }

 private:
  std::string name_ = "BASELINE_BTree_Disk";
  std::string index_file_;
  BTree btree_;
  int inner_on_disk_num_;

  int cnt_;
};

#endif  // !INDEXES_BTREE_DISK_H_