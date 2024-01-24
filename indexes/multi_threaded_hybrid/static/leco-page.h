#ifndef INDEXES_HYBRID_STATIC_MULTI_THREADED_LECO_PAGE_H_
#define INDEXES_HYBRID_STATIC_MULTI_THREADED_LECO_PAGE_H_

#include "../../../libraries/LeCo/headers/codecfactory.h"
#include "../../../libraries/LeCo/headers/common.h"
#include "../../../libraries/LeCo/headers/piecewise_fix_integer_template.h"
#include "../../../libraries/LeCo/headers/piecewise_fix_integer_template_float.h"
#include "./static_base.h"

using namespace Codecset;

template <typename K, typename V>
class MultiThreadedStaticLecoPage : public MultiThreadedStaticIndex<K, V> {
 public:
  struct param_t {
    size_t record_per_page_;
    size_t fix_page_;
    size_t slide_page_;
    size_t block_num_;

    typename MultiThreadedStaticIndex<K, V>::param_t disk_params;
  };
  typedef MultiThreadedStaticIndex<K, V> Base;

  MultiThreadedStaticLecoPage(param_t p) : Base(p.disk_params), params_(p) {}

  class LeCoZonemap {
   public:
    LeCoZonemap(){};
    LeCoZonemap(param_t p)
        : record_per_page_(p.record_per_page_),
          fixed_pages_(p.fix_page_),
          slide_pages_(p.slide_page_),
          block_num_(p.block_num_) {}

    LeCoZonemap& operator=(const LeCoZonemap& other) {
      codec_ = other.codec_;
      block_start_vec_ = other.block_start_vec_;
      block_width_ = other.block_width_;
      point_num_ = other.point_num_;
      max_y_ = other.max_y_;

      record_per_page_ = other.record_per_page_;
      fixed_pages_ = other.fixed_pages_;
      slide_pages_ = other.slide_pages_;

      block_num_ = other.block_num_;
      memory_size_ = other.memory_size_;

      return *this;
    }

    void Build(std::vector<std::pair<K, V>>& train_data) {
      // rebuild the static index
      codec_ = Leco_int<K>();
      block_start_vec_ = std::vector<uint8_t*>();
      max_y_ = train_data.back().second;
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
        block_num_ = 8;
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

#ifdef PRINT_PROCESSING_INFO
      std::cout << "\nLeco-page use " << block_num_ << " models for "
                << train_data.size() << " records"
                << ",\t" << PRINT_MIB(GetNodeSize()) << " MiB" << std::endl;
#endif  // PRINT_PROCESSING_INFO
    }

    SearchRange FindRange(const K key) {
      size_t pos = LecoBinarySearch(key);
      size_t start = pos * (fixed_pages_ + slide_pages_);
      if (pos >= slide_pages_) {
        start -= slide_pages_;
      }
      size_t end = start + fixed_pages_ + 2 * slide_pages_;
      return {start * record_per_page_,
              std::min(max_y_ + 1, end * record_per_page_)};
    }

    size_t GetNodeSize() const { return memory_size_; }

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
    size_t memory_size_ = 0;
  };

  size_t GetStaticInitSize(typename Base::DataVec_& data) const {
    LeCoZonemap leco(params_);
    leco.Build(data);
    return leco.GetNodeSize();
  }

  MultiThreadedStaticLecoPage& operator=(
      const MultiThreadedStaticLecoPage& other) {
    Base::operator=(other);
    leco_ = std::vector<LeCoZonemap>(Base::merge_thread_num_);
    for (size_t i = 0; i < Base::merge_thread_num_; i++) {
      leco_[i] = other.leco_[i];
    }
    total_index_size_.store(other.total_index_size_.load());
    params_ = other.params_;

    return *this;
  }

  void Build(typename Base::DataVec_& data, int thread_id) {
    // merge data
    Base::Init(data, thread_id);

    // rebuild the static index
    LeCoZonemap tmp(params_);
    leco_ = std::vector<LeCoZonemap>(Base::merge_thread_num_);
    int s = 0, e = 0;
    int sub_item_num = std::ceil(data.size() * 1.0 / Base::merge_thread_num_);
    total_index_size_ = 0;
    for (int i = 0; i < Base::merge_thread_num_; i++) {
      leco_[i] = tmp;
      s = i * sub_item_num;
      e = std::min((i + 1) * sub_item_num, static_cast<int>(data.size()));
      typename Base::DataVec_ train_data(data.begin() + s, data.begin() + e);
      for (size_t j = 0; j < train_data.size(); j++) {
        train_data[j].second = j;
      }
      leco_[i].Build(train_data);
      total_index_size_.fetch_add(leco_[i].GetNodeSize());

#ifdef PRINT_PROCESSING_INFO
      std::cout << "\nLeco-page use " << block_num_ << " models for "
                << train_data.size() << " records"
                << ",\t" << PRINT_MIB(GetNodeSize()) << " MiB" << std::endl;
#endif  // PRINT_PROCESSING_INFO
    }
  }
  void Merge(typename Base::DataVec_& data, int thread_id) {
    Base::PartitionData(data, thread_id);
    total_index_size_ = 0;
    auto pid = Base::ObtainMergeTask(thread_id);
    if (pid >= 0) {
      MergeSubData(data, thread_id, pid);
    }
  }

  void MergeSubData(typename Base::DataVec_& data, int thread_id,
                    int partition_id) {
    // merge data
    typename Base::DataVec_ train_data;
    Base::MergeSubData(data, train_data, thread_id, partition_id);

#ifdef BREAKDOWN
    auto start = std::chrono::high_resolution_clock::now();
#endif
    // rebuild the static index
    leco_[partition_id] = LeCoZonemap(params_);
    leco_[partition_id].Build(train_data);
#ifdef BREAKDOWN
    auto end = std::chrono::high_resolution_clock::now();
    Base::model_training_lat[thread_id] +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
#endif

    total_index_size_.fetch_add(leco_[partition_id].GetNodeSize());

#ifdef PRINT_PROCESSING_INFO
    std::cout << "\nLeco-page use " << block_num_ << " models for "
              << train_data.size() << " records"
              << ",\t" << PRINT_MIB(GetNodeSize()) << " MiB" << std::endl;
#endif  // PRINT_PROCESSING_INFO
    Base::FinishMerge(thread_id);
  }

  V Find(const K key, int thread_id) {
    auto pid = Base::GetPartitionID(key);
    auto range = leco_[pid].FindRange(key);
    return Base::FindData(range, key, thread_id, pid);
  }

  bool Update(const K key, const V value, int thread_id) {
    auto pid = Base::GetPartitionID(key);
    auto range = leco_[pid].FindRange(key, thread_id);
    return Base::UpdateData(range, key, value, thread_id, pid);
  }

  V Scan(const K key, const int length, int thread_id) {
    auto pid = Base::GetPartitionID(key);
    auto range = leco_[pid].FindRange(key);
    return Base::ScanData(range, key, length, thread_id, pid);
  }

  inline size_t size() const { return Base::size(); }

  inline size_t GetNodeSize() const { return total_index_size_.load(); }

  inline size_t GetTotalSize() const {
    return GetNodeSize() + Base::GetDiskBytes();
  }

  void PrintEachPartSize() {
    std::cout << "\t\tleco-page:" << PRINT_MIB(GetNodeSize())
              << ",\ton-disk data num:" << size()
              << ",\ton-disk MiB:" << PRINT_MIB(Base::GetDiskBytes())
              << ",\ttotal MiB:" << PRINT_MIB(GetTotalSize()) << std::endl;
  }

  param_t GetIndexParams() const {
    return params_.record_per_page_ *
           (params_.fix_page_ + 2 * params_.slide_page_);
  }

  std::string GetIndexName() const {
    return "MultiThreadedStaticLecoPage-" +
           std::to_string((params_.fix_page_ + 2 * params_.slide_page_) *
                          params_.record_per_page_);
  }

 private:
  std::vector<LeCoZonemap> leco_;
  std::atomic<size_t> total_index_size_;
  param_t params_;
};

#endif