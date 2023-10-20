
#ifndef INDEXES_HYBRID_DYNAMIC_DYNAMIC_BASE_H_
#define INDEXES_HYBRID_DYNAMIC_DYNAMIC_BASE_H_

#include <string>
#include <utility>
#include <vector>

template <typename K, typename V>
class DynamicIndex {
 public:
  DynamicIndex() {}

  typedef K K_;
  typedef V V_;
  typedef std::pair<K_, V_> Record_;
  typedef std::vector<Record_> DataVev_;

  virtual void Build(DataVev_& data) = 0;
  virtual void Merge(DataVev_& merged_data, uint64_t num) = 0;

  virtual V Find(const K key) const = 0;
  virtual V Scan(const K key, const int range) const = 0;

  virtual bool Insert(const K key, const V value) = 0;
  // virtual bool Update(const K key, const V value) = 0;
  virtual bool Delete(const K key) = 0;

  virtual size_t size() const = 0;
  virtual size_t GetNodeSize() const = 0;
  virtual size_t GetTotalSize() const = 0;

  virtual std::string GetIndexName() const { return name_; }

 private:
  std::string name_ = "DYNAMIC_BASE";
};

#endif  // INDEXES_HYBRID_DYNAMIC_DYNAMIC_BASE_H_
