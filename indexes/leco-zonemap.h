#ifndef INDEXES_LECO_ZONEMAP_H_
#define INDEXES_LECO_ZONEMAP_H_

#include <string>
#include <utility>
#include <vector>

#include "../libraries/LeCo/headers/codecfactory.h"
#include "../libraries/LeCo/headers/common.h"
#include "../libraries/LeCo/headers/piecewise_fix_integer_template.h"
#include "../libraries/LeCo/headers/piecewise_fix_integer_template_float.h"
#include "./index.h"
using namespace Codecset;

template <typename K, typename V>
class LecoZonemap : public BaseIndex<K, V> {
 public:
  struct param_t {
    size_t record_per_page_;
    size_t tolerance_;
    size_t block_num_;
  };

  LecoZonemap(param_t p = param_t{256, 1, 1000})
      : tolerance_(p.tolerance_),
        record_per_page_(p.record_per_page_),
        block_num_(p.block_num_) {}

  void Build(std::vector<std::pair<K, V>>& data) {
    std::vector<K> points;
    int seg = record_per_page_ * tolerance_;
    for (size_t i = seg - 1; i < data.size(); i += seg) {
      points.push_back(data[i].first);
    }
    if (points.back() != data.back().first) {
      points.push_back(data.back().first);
    }
    point_num_ = points.size();
    if (point_num_ < block_num_) {
      block_num_ = 10;
    }

    block_width_ = point_num_ / block_num_;
    block_num_ = point_num_ / block_width_;
    if (block_num_ * block_width_ < point_num_) {
      block_num_++;
    }  // handle with the last block, maybe < block_width_
    codec_.init(block_num_, block_width_);

    Leco_int<K> origin_codec;
    origin_codec.init(block_num_, block_width_);

    memory_size_ = 0;
    for (size_t i = 0; i < block_num_; i++) {
      int block_length = block_width_;
      if (i == block_num_ - 1) {
        block_length = point_num_ - (block_num_ - 1) * block_width_;
      }

      uint8_t* descriptor = (uint8_t*)malloc(block_length * sizeof(K) * 4);
      uint8_t* res = descriptor;
      res = codec_.encodeArray8_int(points.data() + (i * block_width_),
                                    block_length, descriptor, i);
      uint32_t segment_size = res - descriptor;
      descriptor = (uint8_t*)realloc(descriptor, segment_size);
      block_start_vec_.push_back(descriptor);
      memory_size_ += segment_size;
    }

    disk_size_ = (sizeof(K) + sizeof(V)) * data.size();
  }

  SearchRange Lookup(const K lookup_key) const {};

  SearchRange Lookup(const K lookup_key) {
    size_t pos = LecoBinarySearch(lookup_key);
    return {pos * record_per_page_ * tolerance_,
            (pos + 1) * record_per_page_ * tolerance_};
  }

  size_t GetIndexParams() const override {
    return record_per_page_ * tolerance_;
  }

  std::string GetIndexName() const override {
    return "LecoZonemap_" + std::to_string(tolerance_ * record_per_page_);
  }

  size_t GetModelNum() const override { return block_num_; }

  size_t GetInMemorySize() const override { return memory_size_; }

  size_t GetIndexSize() const override {
    return GetInMemorySize() + disk_size_;
  }

 private:
  size_t LecoBinarySearch(K key) {
    uint64_t s = 0, e = point_num_;
    while (s < e) {
      uint64_t mid = (s + e) >> 1;
      K data_mid =
          codec_.randomdecodeArray8(block_start_vec_[mid / block_width_],
                                    mid % block_width_, NULL, point_num_);
      if (data_mid < key)
        s = mid + 1;
      else
        e = mid;
    }
    return s;
  }

 private:
  Leco_int<K> codec_;
  std::vector<uint8_t*> block_start_vec_;

  int block_width_;
  size_t block_num_;
  size_t point_num_;

  size_t disk_size_ = 0;
  size_t memory_size_ = 0;
  size_t tolerance_;
  size_t record_per_page_;
};

#endif  // INDEXES_LECO_ZONEMAP_H_
