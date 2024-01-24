#ifndef INDEXES_ALEX_DISK_H_
#define INDEXES_ALEX_DISK_H_

#include "../../libraries/UpdatableLearnedIndexDisk/ALEX/alex.h"
#include "../base_index.h"

template <typename K, typename V>
class BaselineAlexDisk : public BaseIndex<K, V> {
 public:
  struct param_t {
    std::string main_file;
  };

  BaselineAlexDisk(param_t params = param_t("t"))
      : index_file_(params.main_file),
        alex_(HYBRID_MODE, true, const_cast<char*>(index_file_.c_str()),
              const_cast<char*>((index_file_ + "_data").c_str())),
        block_cnt_(0),
        level_cnt_(0),
        search_time(0),
        insert_time(0),
        smo_time(0),
        maintain_time(0),
        smo_count(0) {}

  void Build(typename BaseIndex<K, V>::DataVec_& data) {
    alex_.bulk_load(data.data(), data.size());
    alex_.sync_metanode(true);
    alex_.sync_metanode(false);
    std::cout << "\nALEX use " << alex_.stats_.num_model_nodes << " models for "
              << data.size() << " records" << std::endl;
  }

  V Find(const K key) {
    int c = 0, l = 0, inner = 0;
    V res = 0;
    alex_.get_leaf_disk(key, &res, &c, &l, &inner);
    block_cnt_ += c;
    level_cnt_ += l;
    return res;
  }

  V Scan(const K key, const int range) {
    int c = 0;
    V res[range];
    alex_.scan(res, key, &c, range);
    block_cnt_ += c;
    V sum = 0;
    for (int i = 0; i < range; i++) {
      sum += res[i];
    }
    return sum;
  }

  bool Insert(const K key, const V value) {
    long long sel = 0, sml = 0, inl = 0, mal = 0;
    int smo = 0;

    alex_.insert_disk(key, value, &sel, &inl, &sml, &mal, &smo);

    search_time += sel;
    insert_time += sml;
    smo_time += inl;
    maintain_time += mal;
    smo_count += smo;
    return true;
  }
  // TODO: update
  bool Update(const K key, const V value) { return Insert(key, value); }
  bool Delete(const K key) {
    alex_.erase(key);
    return true;
  }

  size_t GetNodeSize() { return alex_.get_memory_size(); }

  size_t GetTotalSize() { return GetNodeSize() + alex_.get_file_size(); }

  void PrintEachPartSize() {
    std::cout << "\t\talex in-memory size:" << PRINT_MIB(GetNodeSize())
              << ",\talex on-disk size:" << PRINT_MIB(alex_.get_file_size())
              << ",\ttotal MiB:" << PRINT_MIB(GetTotalSize()) << std::endl;
  }

  std::string GetIndexName() const { return name_; }

  param_t GetIndexParams() const { return param_t(0); }

 private:
  std::string name_ = "BASELINE_ALEX_DISK";
  std::string index_file_;
  alex_disk::Alex<K, V> alex_;

  int block_cnt_;
  int level_cnt_;
  long long search_time;
  long long insert_time;
  long long smo_time;
  long long maintain_time;
  int smo_count;
};

#endif  // !INDEXES_ALEX_DISK_H_