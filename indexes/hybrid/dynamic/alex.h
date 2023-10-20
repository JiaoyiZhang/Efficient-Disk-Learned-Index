#ifndef INDEXES_HYBRID_DYNAMIC_ALEX_H_
#define INDEXES_HYBRID_DYNAMIC_ALEX_H_

#include <string>
#include <utility>
#include <vector>

#include "./alex/alex_map.h"
#include "./dynamic_base.h"

template <typename K, typename V>
class AlexIndex : public DynamicIndex<K, V> {
 public:
  struct param_t {};
  AlexIndex(param_t) {}

  typedef K K_;
  typedef V V_;
  typedef std::pair<K_, V_> Record_;
  typedef std::vector<Record_> DataVev_;

  void Build(DataVev_& data) {
    alex_ = alex::Alex<K, V>();
    alex_.bulk_load(data.data(), data.size());
#ifdef PRINT_PROCESSING_INFO
    std::cout << "\nALEX use " << alex_.stats_.num_model_nodes << " models for "
              << data.size() << " records"
              << ",\t" << PRINT_MIB(GetNodeSize())
              << " MiB,\ttotal size:" << PRINT_MIB(GetTotalSize()) << std::endl;
#endif  // PRINT_PROCESSING_INFO
  }

  V Find(const K key) const {
    auto it = alex_.lower_bound(key);
    if (it == alex_.cend() || it.key() != key) {
      return std::numeric_limits<V>::max();
    }
    return it.payload();
  }

  V Scan(const K key, const int range) const {
    auto it = alex_.lower_bound(key);
    if (it == alex_.cend() || it.key() != key) {
      return std::numeric_limits<V>::max();
    }
    V sum = it.payload();
    for (int i = 0; i < range; i++) {
      it++;
      if (it == alex_.cend()) {
        break;
      }
      sum += it.payload();
    }
    return sum;
  }

  bool Insert(const K key, const V value) {
    auto it = alex_.find(key);
    if (it != alex_.end()) {
      alex_.erase(it);
    }
    alex_.insert(key, value);
    return true;
  }

  bool Update(const K key, const V value) {
    auto it = alex_.find(key);
    if (it == alex_.end()) {
      return false;
    }
    alex_.erase(it);
    alex_.insert(key, value);
    return true;
  }

  bool Delete(const K key) {
    alex_.erase(key);
    return true;
  }

  void Merge(DataVev_& merged_data, uint64_t num) {
    merged_data.resize(alex_.size() - num);

    uint64_t seg = alex_.size() / num;
    DataVev_ init_data(std::ceil(alex_.size() * 1.0 / seg));
    uint64_t init_cnt = 0, merge_cnt = 0, total_cnt = 0;

    auto it = alex_.begin();
    while (it != alex_.end()) {
      if (total_cnt % seg == 0) {
        init_data[init_cnt++] = {it.key(), it.payload()};
      } else {
        merged_data[merge_cnt++] = {it.key(), it.payload()};
      }
      it++;
      total_cnt++;
    }
    init_data.resize(init_cnt);
    merged_data.resize(merge_cnt);

    Build(init_data);
  }

  size_t size() const { return alex_.size(); }

  size_t GetNodeSize() const { return alex_.model_size(); }

  size_t GetTotalSize() const { return alex_.model_size() + alex_.data_size(); }

  void PrintEachPartSize() {
    std::cout << "\t\talex model size:" << PRINT_MIB(alex_.model_size())
              << ",\talex data size:" << PRINT_MIB(alex_.data_size())
              << ",\tin-memory data num:" << alex_.size() << ",\tin-memory MiB:"
              << PRINT_MIB(sizeof(Record_) * alex_.size())
              << ",\ttotal MiB:" << PRINT_MIB(GetTotalSize()) << std::endl;
  }

  std::string GetIndexName() const { return name_; }

  param_t GetIndexParams() const { return param_t(0); }

 private:
  std::string name_ = "ALEX";
  alex::Alex<K, V> alex_;
};

#endif  // INDEXES_HYBRID_DYNAMIC_ALEX_H_
