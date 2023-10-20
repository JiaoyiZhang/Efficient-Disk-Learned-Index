#ifndef INDEXES_HYBRID_STATIC_PGM_H_
#define INDEXES_HYBRID_STATIC_PGM_H_

#include "./pgm/pgm_index_variants.hpp"
#include "./static_base.h"

template <typename K, typename V>
class StaticPGMIndex : public StaticIndex<K, V> {
 public:
  struct param_t {
    uint64_t epsilon;

    typename StaticIndex<K, V>::param_t disk_params;
  };

  StaticPGMIndex(param_t p)
      : StaticIndex<K, V>(p.disk_params), epsilon_(p.epsilon) {}

  size_t GetStaticInitSize(typename StaticIndex<K, V>::DataVec_& data) const {
    pgm::CompressedPGMIndex<K> pgm(data.begin(), data.end(), epsilon_);
    return pgm.size_in_bytes();
  }

  void Build(typename StaticIndex<K, V>::DataVec_& data) {
    // merge data
    typename StaticIndex<K, V>::DataVec_ train_data;
    StaticIndex<K, V>::MergeData(data, train_data);

    // rebuild the static index
    pgm_ = pgm::CompressedPGMIndex<K>(train_data.begin(), train_data.end(),
                                      epsilon_);
#ifdef PRINT_PROCESSING_INFO
    std::cout << "\nPGM use " << pgm_.segments_count() << " models for "
              << train_data.size() << " records"
              << ",\t" << PRINT_MIB(GetNodeSize()) << " MiB" << std::endl;
#endif  // PRINT_PROCESSING_INFO
  }

  V Find(const K key) {
    auto range = pgm_.search(key);
    return StaticIndex<K, V>::FindData({range.lo, range.hi}, key);
  }

  V Scan(const K key, const int length) {
    auto range = pgm_.search(key);
    return StaticIndex<K, V>::ScanData({range.lo, range.hi}, key, length);
  }

  bool Update(const K key, const V value) {
    auto range = pgm_.search(key);
    return StaticIndex<K, V>::UpdateData({range.lo, range.hi}, key, value);
  }

  size_t size() const { return StaticIndex<K, V>::size(); }

  size_t GetNodeSize() const { return pgm_.size_in_bytes(); }

  size_t GetTotalSize() const {
    return pgm_.size_in_bytes() +
           sizeof(typename StaticIndex<K, V>::Record_) * size();
  }

  void PrintEachPartSize() {
    std::cout << "\t\tpgm:" << PRINT_MIB(pgm_.size_in_bytes())
              << ",\ton-disk data num:" << size() << ",\ton-disk MiB:"
              << PRINT_MIB(sizeof(typename StaticIndex<K, V>::Record_) * size())
              << ",\ttotal MiB:" << PRINT_MIB(GetTotalSize()) << std::endl;
  }

  param_t GetIndexParams() const { return epsilon_; }

  std::string GetIndexName() const {
    return "StaticPGM-" + std::to_string(epsilon_);
  }

 private:
  pgm::CompressedPGMIndex<K> pgm_;

  size_t epsilon_;
};

#endif