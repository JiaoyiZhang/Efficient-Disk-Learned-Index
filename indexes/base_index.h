#ifndef INDEXES_BASE_INDEX_H_
#define INDEXES_BASE_INDEX_H_

#include <iostream>
#include <vector>

#include "../ycsb_utils/structures.h"
#include "../ycsb_utils/util_lid.h"

template <typename K, typename V>
class BaseIndex {
 public:
  struct param_t {};

  typedef K K_;
  typedef V V_;
  typedef std::pair<K_, V_> Record_;
  typedef std::vector<Record_> DataVec_;

  virtual void Build(DataVec_& new_data) = 0;

  virtual V Find(const K key) = 0;

  virtual V Scan(const K key, const int range) = 0;

  virtual bool Insert(const K key, const V value) = 0;
  virtual bool Update(const K key, const V value) = 0;
  virtual bool Delete(const K key) = 0;

  size_t GetNodeSize(){};
  size_t GetTotalSize(){};
  virtual std::string GetIndexName() const = 0;
};

template <typename K, typename V>
class MultiThreadedBaseIndex {
 public:
  struct param_t {};

  typedef K K_;
  typedef V V_;
  typedef std::pair<K_, V_> Record_;
  typedef std::vector<Record_> DataVec_;

  virtual void Build(DataVec_& new_data) = 0;

  virtual V Find(const K key, const int thread_id) = 0;

  virtual V Scan(const K key, const int range, const int thread_id) = 0;

  virtual bool Insert(const K key, const V value, const int thread_id) = 0;
  virtual bool Update(const K key, const V value, const int thread_id) = 0;
  virtual bool Delete(const K key, const int thread_id) = 0;

  size_t GetNodeSize(){};
  size_t GetTotalSize(){};
  virtual std::string GetIndexName() const = 0;
};

#endif  // !INDEXES_BASE_INDEX_H_