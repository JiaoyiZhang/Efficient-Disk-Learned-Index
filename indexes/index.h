#ifndef INDEXES_INDEX_H_
#define INDEXES_INDEX_H_

#include <string>
#include <utility>
#include <vector>

#include "../experiments/util.h"

template <typename K, typename V>
class BaseIndex {
 public:
  BaseIndex() {}

  typedef K K_;
  typedef V V_;
  typedef std::pair<K_, V_> Record_;
  typedef std::vector<Record_> DataVev_;

  virtual void Build(DataVev_& data) = 0;

  // Return the search range, the start position must not be less than 0, and
  // the end postion must be less than the data_num_

  virtual SearchRange Lookup(const K lookup_key) const = 0;

  virtual size_t GetIndexParams() const { return 0; }

  virtual std::string GetIndexName() const { return name_; }

  virtual size_t GetInMemorySize() const { return 0; }

  virtual size_t GetIndexSize() const { return disk_size_; }

  virtual size_t GetModelNum() const = 0;

 private:
  std::string name_ = "Basic Index";
  size_t disk_size_ = 0;
};

#endif  // INDEXES_INDEX_H_
