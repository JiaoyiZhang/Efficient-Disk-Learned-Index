#ifndef INDEXES_HYBRID_STATIC_LECO_PAGE_H_
#define INDEXES_HYBRID_STATIC_LECO_PAGE_H_

#include "../../../libraries/LeCo/headers/codecfactory.h"
#include "../../../libraries/LeCo/headers/common.h"
#include "../../../libraries/LeCo/headers/piecewise_fix_integer_template.h"
#include "../../../libraries/LeCo/headers/piecewise_fix_integer_template_float.h"
#include "./static_base.h"

using namespace Codecset;

template <typename K, typename V>
class StaticLecoPage : public StaticIndex<K, V> {
 public:
  struct param_t {
    size_t record_per_page_;
    size_t fix_page_;
    size_t slide_page_;
    size_t block_num_;

    typename StaticIndex<K, V>::param_t disk_params;
  };

  StaticLecoPage(param_t p)
      : StaticIndex<K, V>(p.disk_params),
        record_per_page_(p.record_per_page_),
        fixed_pages_(p.fix_page_),
        slide_pages_(p.slide_page_),
        block_num_(p.block_num_),
        memory_size_(0),
        disk_size_(0) {}

  size_t GetStaticInitSize(typename StaticIndex<K, V>::DataVec_& data) const {
    Leco_int<K> codec;
    std::vector<uint8_t*> block_start_vec;
    std::vector<K> upper_bounds, lower_bounds, training;
    int group_width = record_per_page_ * (fixed_pages_ + slide_pages_);
    int slide_width = record_per_page_ * slide_pages_;
    if (fixed_pages_) {
      slide_width += 1;
    }

    for (int i = group_width; i < data.size(); i += group_width) {
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
    auto point_num = lower_bounds.size();
    auto block_width = point_num / block_num_;
    auto block_num = point_num / block_width;
    if (block_num * block_width < point_num) {
      block_num++;
    }
    codec.init(block_num, block_width);

    size_t memory_size = 0;
    for (size_t i = 0; i < block_num; i++) {
      int block_length = block_width;
      if (i == block_num - 1) {
        block_length = point_num - (block_num - 1) * block_width;
      }

      uint8_t* descriptor = (uint8_t*)malloc(block_length * sizeof(K) * 4);
      uint8_t* res = descriptor;
      res = codec.encodeArray8_int(lower_bounds.data() + (i * block_width),
                                   upper_bounds.data() + (i * block_width),
                                   training.data() + (i * block_width),
                                   block_length, descriptor, i);
      uint32_t segment_size = res - descriptor;
      descriptor = (uint8_t*)realloc(descriptor, segment_size);
      block_start_vec.push_back(descriptor);
      memory_size += segment_size;
    }
    return memory_size;
  }

  void Build(typename StaticIndex<K, V>::DataVec_& data) {
    // merge data

#ifdef BREAKDOWN
    auto start = std::chrono::high_resolution_clock::now();
#endif
    typename StaticIndex<K, V>::DataVec_ train_data;
    StaticIndex<K, V>::MergeData(data, train_data);
#ifdef BREAKDOWN
    auto end = std::chrono::high_resolution_clock::now();
    if (merge_cnt >= 0) {
      merge_lat +=
          std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
              .count();
    }
    start = std::chrono::high_resolution_clock::now();
#endif

    // rebuild the static index
    codec_ = Leco_int<K>();
    block_start_vec_ = std::vector<uint8_t*>();
    max_y_ = train_data.size() - 1;
    std::vector<K> upper_bounds, lower_bounds, training;
    int group_width = record_per_page_ * (fixed_pages_ + slide_pages_);
    int slide_width = record_per_page_ * slide_pages_;
    if (fixed_pages_) {
      slide_width += 1;
    }

#ifdef PRINT_PROCESSING_INFO
    std::cout << "fixed_pages_:" << fixed_pages_
              << ",\tslide_pages_:" << slide_pages_
              << ",\tslide_width:" << slide_width << std::endl;
#endif  // PRINT_PROCESSING_INFO

    for (int i = group_width; i < train_data.size(); i += group_width) {
      if (i < slide_width) {
        lower_bounds.push_back(train_data[0].first);
      } else {
        lower_bounds.push_back(train_data[i - slide_width].first);
      }
      K upper_key = train_data[i - 1].first;
      if (train_data[i].first >= 1) {
        upper_key = std::max(train_data[i].first - 1, upper_key);
      }
      upper_bounds.push_back(upper_key);
      training.push_back((upper_key + lower_bounds.back()) / 2);
    }
    if (upper_bounds.back() < train_data.back().first) {
      lower_bounds.push_back(train_data.back().first);
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
    disk_size_ = sizeof(typename StaticIndex<K, V>::Record_) * size();
#ifdef BREAKDOWN
    end = std::chrono::high_resolution_clock::now();
    if (merge_cnt >= 0) {
      train_lat +=
          std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
              .count();
    }
    merge_cnt++;
#endif

#ifdef PRINT_PROCESSING_INFO
    std::cout << "\nLeco-page use " << block_num_ << " models for "
              << train_data.size() << " records"
              << ",\t" << PRINT_MIB(GetNodeSize()) << " MiB" << std::endl;
#endif  // PRINT_PROCESSING_INFO
  }

  V Find(const K key) {
    size_t pos = LecoBinarySearch(key);
    size_t start = pos * (fixed_pages_ + slide_pages_);
    if (pos >= slide_pages_) {
      start -= slide_pages_;
    }
    size_t end = start + fixed_pages_ + 2 * slide_pages_;
    return StaticIndex<K, V>::FindData(
        {start * record_per_page_,
         std::min(max_y_ + 1, end * record_per_page_)},
        key);
  }

  bool Update(const K key, const V value) {
    size_t pos = LecoBinarySearch(key);
    size_t start = pos * (fixed_pages_ + slide_pages_);
    if (pos >= slide_pages_) {
      start -= slide_pages_;
    }
    size_t end = start + fixed_pages_ + 2 * slide_pages_;
    return StaticIndex<K, V>::UpdateData(
        {start * record_per_page_,
         std::min(max_y_ + 1, end * record_per_page_)},
        key, value);
  }

  V Scan(const K key, const int length) {
    size_t pos = LecoBinarySearch(key);
    size_t start = pos * (fixed_pages_ + slide_pages_);
    if (pos >= slide_pages_) {
      start -= slide_pages_;
    }
    size_t end = start + fixed_pages_ + 2 * slide_pages_;
    return StaticIndex<K, V>::ScanData(
        {start * record_per_page_,
         std::min(max_y_ + 1, end * record_per_page_)},
        key, length);
  }

  inline size_t size() const { return StaticIndex<K, V>::size(); }

  inline size_t GetNodeSize() const { return memory_size_; }

  inline size_t GetTotalSize() const { return memory_size_ + disk_size_; }

  void PrintEachPartSize() {
    std::cout << "\t\tleco-page:" << PRINT_MIB(GetNodeSize())
              << ",\ton-disk data num:" << size() << ",\ton-disk MiB:"
              << PRINT_MIB(sizeof(typename StaticIndex<K, V>::Record_) * size())
              << ",\ttotal MiB:" << PRINT_MIB(GetTotalSize()) << std::endl;
#ifdef BREAKDOWN
    if (merge_cnt > 0) {
      std::cout << "merge cnt:" << merge_cnt << std::endl;
      std::cout << " merge avg latency:" << merge_lat / merge_cnt / 1e6 << " ms"
                << std::endl;
      std::cout << " train avg latency:" << train_lat / merge_cnt / 1e6 << " ms"
                << std::endl;
      std::cout << "-------------print over---------------" << std::endl;
    }
    StaticIndex<K, V>::Breakdown();
#endif
  }

  param_t GetIndexParams() const {
    return record_per_page_ * (fixed_pages_ + 2 * slide_pages_);
  }

  std::string GetIndexName() const {
    return "StaticLecoPage-" +
           std::to_string((fixed_pages_ + 2 * slide_pages_) * record_per_page_);
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

#ifdef BREAKDOWN
  double merge_lat = 0.0;
  double train_lat = 0.0;
  int merge_cnt = -1;
#endif

  size_t record_per_page_;
  size_t fixed_pages_;
  size_t slide_pages_;

  size_t block_num_;
  size_t memory_size_ = 0;
  size_t disk_size_ = 0;
};

#endif