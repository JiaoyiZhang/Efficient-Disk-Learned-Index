#ifndef INDEXES_LECO_PAGE_H_
#define INDEXES_LECO_PAGE_H_

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
class LecoPage : public BaseIndex<K, V> {
 public:
  struct param_t {
    size_t record_per_page_;
    size_t fix_page_;
    size_t slide_page_;
    size_t block_num_;
  };

  LecoPage(param_t p = param_t{256, 1, 1, 1000})
      : record_per_page_(p.record_per_page_),
        fixed_pages_(p.fix_page_),
        slide_pages_(p.slide_page_),
        block_num_(p.block_num_) {}

  void Build(std::vector<std::pair<K, V>>& data) {
    max_y_ = data.back().second;
    std::vector<K> upper_bounds, lower_bounds, training;
    int group_width = record_per_page_ * (fixed_pages_ + slide_pages_);
    int slide_width = record_per_page_ * slide_pages_;
    if (fixed_pages_) {
      slide_width += 1;
    }
    std::cout << "fixed_pages_:" << fixed_pages_
              << ",\tslide_pages_:" << slide_pages_
              << ",\tslide_width:" << slide_width << std::endl;

    for (size_t i = group_width; i < data.size(); i += group_width) {
      if (i < slide_width) {
        lower_bounds.push_back(data[0].first);
      } else {
        lower_bounds.push_back(data[i - slide_width].first);
      }
      K upper_key = data[i - 1].first;
      if (data[i].first >= 1) {
        upper_key = std::max(data[i].first - 1, upper_key);
      }
      upper_bounds.push_back(upper_key);
      training.push_back((upper_key + lower_bounds.back()) / 2);
    }
    if (upper_bounds.back() < data.back().first) {
      lower_bounds.push_back(data.back().first);
      upper_bounds.push_back(std::numeric_limits<K>::max());
      training.push_back(lower_bounds.back());
    }
    point_num_ = lower_bounds.size();
    if (point_num_ < block_num_) {
      block_num_ = 10;
    }

    block_width_ = point_num_ / block_num_;
    block_num_ = point_num_ / block_width_;
    if (block_num_ * block_width_ < point_num_) {
      block_num_++;
    }  // handle with the last block, maybe < block_width_
    codec_.init(block_num_, block_width_);

    memory_size_ = 0;
    for (size_t i = 0; i < block_num_; i++) {
      int block_length = block_width_;
      if (i == block_num_ - 1) {
        block_length = point_num_ - (block_num_ - 1) * block_width_;
      }

      uint8_t* descriptor = (uint8_t*)malloc(block_length * sizeof(K) * 4);
      uint8_t* res = descriptor;
      res = codec_.encodeArray8_int(lower_bounds.data() + (i * block_width_),
                                    upper_bounds.data() + (i * block_width_),
                                    training.data() + (i * block_width_),
                                    block_length, descriptor, i);
      uint32_t segment_size = res - descriptor;
      descriptor = (uint8_t*)realloc(descriptor, segment_size);
      block_start_vec_.push_back(descriptor);
      memory_size_ += segment_size;
    }

    disk_size_ = (sizeof(K) + sizeof(V)) * data.size();

    // //==============================================================
    // // TEST LECO
    // //==============================================================
    // for (size_t i = 0; i < point_num_ - 1; i++) {
    //   K data_mid =
    //       codec_.randomdecodeArray8Page(block_start_vec_[i / block_width_],
    //                                     i % block_width_, NULL, point_num_);

    //   if (data_mid < lower_bounds[i] || data_mid >= lower_bounds[i + 1]) {
    //     std::cout << i << ",\tlower_bounds[i]:" << lower_bounds[i]
    //               << ",\tdata_mid:" << data_mid
    //               << ",\tupper:" << upper_bounds[i]
    //               << ",\tlower_bounds[i+1]:" << lower_bounds[i + 1]
    //               << std::endl;
    //     codec_.randomdecodeArray8Page(block_start_vec_[i / block_width_],
    //                                   i % block_width_, NULL, point_num_);
    //   }

    //   if (data_mid > upper_bounds[i]) {
    //     std::cout << i << ",\tlower_bounds[i]:" << lower_bounds[i]
    //               << ",\tdata_mid:" << data_mid
    //               << ",\tupper:" << upper_bounds[i]
    //               << ",\tlower_bounds[i+1]:" << lower_bounds[i + 1]
    //               << std::endl;
    //     codec_.randomdecodeArray8Page(block_start_vec_[i / block_width_],
    //                                   i % block_width_, NULL, point_num_);
    //   }
    // }
    // K data_mid = codec_.randomdecodeArray8Page(
    //     block_start_vec_[(point_num_ - 1) / block_width_],
    //     (point_num_ - 1) % block_width_, NULL, point_num_);
    // if (data_mid < lower_bounds[point_num_ - 1]) {
    //   std::cout << point_num_ - 1
    //             << ",\tlower_bounds[i]:" << lower_bounds[point_num_ - 1]
    //             << ",\tdata_mid:" << data_mid << std::endl;
    // }
    // //==============================================================
    // // TEST LECO OVER
    // //==============================================================
  }

  SearchRange Lookup(const K lookup_key) const {};

  SearchRange Lookup(const K lookup_key) {
    size_t pos = LecoBinarySearch(lookup_key);
    size_t start = pos * (fixed_pages_ + slide_pages_);
    if (pos >= slide_pages_) {
      start -= slide_pages_;
    }
    size_t end = start + fixed_pages_ + 2 * slide_pages_;
    return {start * record_per_page_,
            std::min(max_y_ + 1, end * record_per_page_)};
  }

  size_t GetIndexParams() const override {
    return record_per_page_ * (fixed_pages_ + 2 * slide_pages_);
  }

  std::string GetIndexName() const override {
    return "LecoPage_" +
           std::to_string((fixed_pages_ + 2 * slide_pages_) * record_per_page_);
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
          codec_.randomdecodeArray8Page(block_start_vec_[mid / block_width_],
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
  size_t point_num_;
  V max_y_;

  size_t record_per_page_;
  size_t fixed_pages_;
  size_t slide_pages_;

  size_t block_num_;
  size_t disk_size_ = 0;
  size_t memory_size_ = 0;
};

#endif  // INDEXES_LECO_PAGE_H_
