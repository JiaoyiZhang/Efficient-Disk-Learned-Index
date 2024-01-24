#ifndef INDEXES_COMPRESSED_DISK_ORIENTED_INDEX_BASE_H_
#define INDEXES_COMPRESSED_DISK_ORIENTED_INDEX_BASE_H_
#include <math.h>

#include <algorithm>
#include <cassert>
#include <iostream>
#include <vector>

#include "../../libraries/LeCo/headers/codecfactory.h"
#include "../../libraries/LeCo/headers/common.h"
#include "../../libraries/LeCo/headers/piecewise_fix_integer_template.h"
#include "../../libraries/LeCo/headers/piecewise_fix_integer_template_float.h"
#include "../PGM-index-disk/pgm_index_page.hpp"
#include "../PGM-index/include/pgm/sdsl.hpp"

namespace compressed_disk_index {

struct SearchBound {
  size_t begin;
  size_t end;  // Exclusive.
};

#define PARALLELISM 64
#define LINEAR_BOUND 5
#define DI_MiB(bytes) ((bytes) / 1024.0 / 1024.0)
// typedef uint32_t INTERCEPT_TYPE;
typedef int64_t INTERCEPT_TYPE;

template <typename K>
class Segments {
 public:
  Segments(){};
  Segments(std::pair<float, float> range,
           std::pair<long double, long double> intersection, K k)
      : slope_range_(range), intersection_(intersection), key_(k){};
  explicit Segments(const Segments& other)
      : slope_range_(other.slope_range_),
        intersection_(other.intersection_),
        key_(other.key_) {}

  Segments& operator=(const Segments& other) {
    slope_range_ = other.slope_range_;
    intersection_ = other.intersection_;
    key_ = other.key_;
    return *this;
  }

 public:
  std::pair<float, float> slope_range_;
  std::pair<long double, long double> intersection_;
  K key_;
};

class CompressedIntercepts {
 public:
  CompressedIntercepts(){};

  template <typename IterI>
  void Compress(IterI first_intercept, IterI last_intercept) {
    if (sizeof(INTERCEPT_TYPE) == sizeof(uint32_t) &&
        *(last_intercept - 1) > UINT32_MAX) {
      std::cout << "UINT32_MAX:" << UINT32_MAX
                << ",\t*(last_intercept - 1):" << *(last_intercept - 1)
                << std::endl;
      throw std::overflow_error(
          "Change the type of CompressedIntercepts::intercept to int64_t");
    }

    intercept_offset_ = *first_intercept;
    // make intercepts increasing
    std::vector<INTERCEPT_TYPE> increasing_intercepts(first_intercept,
                                                      last_intercept);
    INTERCEPT_TYPE offset = 0, idx = 1;
    for (auto it = first_intercept + 1; it != last_intercept; it++, idx++) {
      if (*it <= *(it - 1)) {
        intercepts_map_.insert({it - first_intercept - 1, offset});
        offset += *(it - 1) - *it + 1;
      }
      increasing_intercepts[idx] += offset;
    }
    intercepts_map_.insert({increasing_intercepts.size() - 1, offset});
    if (!is_sorted(increasing_intercepts.begin(),
                   increasing_intercepts.end())) {
      std::cout << "increasing_intercepts is not sorted!" << std::endl;
      std::sort(increasing_intercepts.begin(), increasing_intercepts.end());
    }

    // Compress and store intercepts
    auto max_intercept = increasing_intercepts.back() - intercept_offset_ + 3;
    auto intercepts_count = std::distance(first_intercept, last_intercept) + 1;
    sdsl::sd_vector_builder builder(max_intercept, intercepts_count);
    builder.set(0);
    for (auto it = increasing_intercepts.begin() + 1;
         it != increasing_intercepts.end(); ++it) {
      builder.set((*it) - intercept_offset_);
      // builder.set(std::min(*it, increasing_intercepts.back()) -
      //             intercept_offset_);
    }
    builder.set(max_intercept - 1);
    compressed_intercepts_ = sdsl::sd_vector<>(builder);
    sdsl::util::init_support(sel1_, &compressed_intercepts_);
  }

  inline INTERCEPT_TYPE get_intercept(size_t i) const {
    auto it = intercepts_map_.lower_bound(i);
    return intercept_offset_ + INTERCEPT_TYPE(sel1_(i + 1)) - it->second;
  }

  inline size_t size() const {
    return sizeof(INTERCEPT_TYPE) +
           sdsl::size_in_bytes(compressed_intercepts_) +
           intercepts_map_.size() * (sizeof(size_t) + sizeof(INTERCEPT_TYPE));
  }

 private:
  INTERCEPT_TYPE intercept_offset_;  ///< An offset to make the intercepts start
                                     ///< from 0 in the bitvector.
  sdsl::sd_vector<> compressed_intercepts_;  ///< The compressed bitvector
                                             ///< storing the intercepts.
  sdsl::sd_vector<>::select_1_type
      sel1_;  ///< The select1 succinct data structure on compressed_intercepts.
  std::map<size_t, INTERCEPT_TYPE> intercepts_map_;
};

template <typename K>
class CompressedSlopes {
 public:
  CompressedSlopes(){};

  template <typename T, typename Cmp>
  static std::vector<size_t> sort_indexes(const std::vector<T>& v, Cmp cmp) {
    std::vector<size_t> idx(v.size());
    std::iota(idx.begin(), idx.end(), 0);
    std::sort(idx.begin(), idx.end(), cmp);
    return idx;
  }

  inline std::vector<int64_t> MergeCompress(
      const std::vector<Segments<K>>& segments) {
    size_t n = segments.size();
    std::vector<int64_t> intercepts;
    intercepts.reserve(n);
    slopes_table_ = std::vector<float>();
    slopes_table_.reserve(n);

    auto cmp = [&](auto i1, auto i2) {
      return segments[i1].slope_range_ < segments[i2].slope_range_;
    };
    auto sorted_indexes = sort_indexes(segments, cmp);
    auto [current_min, current_max] = segments[sorted_indexes[0]].slope_range_;

    std::vector<size_t> mapping(n);
    mapping[sorted_indexes[0]] = 0;

    for (size_t i = 1; i < sorted_indexes.size(); ++i) {
      auto [min, max] = segments[sorted_indexes[i]].slope_range_;
      if (min > current_max) {
        auto slope = 0.5 * (current_min + current_max);
        slopes_table_.push_back(slope);
        current_min = min;
        current_max = max;
      } else {
        if (min > current_min) current_min = min;
        if (max < current_max) current_max = max;
      }
      mapping[sorted_indexes[i]] = slopes_table_.size();
    }

    slopes_table_.push_back(0.5 * (current_min + current_max));
#ifndef HYBRID_BENCHMARK
    std::cout << "slopes_table_ size:" << slopes_table_.size() << std::endl;
#endif

    // Compute intercepts
    intercepts.reserve(n);
    for (size_t i = 0; i < n; ++i) {
      auto [i_x, i_y] = segments[i].intersection_;
      auto slope = slopes_table_[mapping[i]];
      auto intercept =
          (INTERCEPT_TYPE)std::round(i_y - (i_x - segments[i].key_) * slope);
      intercepts.push_back(intercept);
    }

    // Compress and store slopes_map
    slopes_map_ = sdsl::int_vector<>(
        mapping.size(), 0, sdsl::bits::hi(slopes_table_.size() - 1) + 1);
    std::copy(mapping.begin(), mapping.end(), slopes_map_.begin());

    return intercepts;
  }

  inline float get_slope(size_t i) const {
    assert(i < slopes_map_.size());
    assert(slopes_map_[i] < slopes_table_.size());
    return slopes_table_[slopes_map_[i]];
  }

  inline size_t size() const {
    return slopes_map_.bit_size() / 8 + slopes_table_.size() * sizeof(float);
  }

 private:
  std::vector<float> slopes_table_;
  sdsl::int_vector<> slopes_map_;
};

class CompressedErrors {
 public:
  CompressedErrors(){};

  inline void Compress(const std::vector<uint16_t>& errors) {
    size_t n = errors.size();
    std::set<uint16_t> error_set(errors.begin(), errors.end());
    std::map<uint16_t, uint32_t> temp_map;
    int cnt = 0;
    for (auto it = error_set.begin(); it != error_set.end(); it++, cnt++) {
      temp_map.insert({*it, cnt});
    }
    std::cout << "error map size:" << temp_map.size() << std::endl;

    std::vector<uint32_t> mapping(n);
    mapping[0] = 0;
    errors_table_.insert(errors_table_.begin(), error_set.begin(),
                         error_set.end());

    for (size_t i = 0; i < errors.size(); ++i) {
      auto it = temp_map.find(errors[i]);
      if (it != temp_map.end()) {
        uint32_t idx = it->second;
        mapping[i] = idx;
      }
    }

    // Compress and store errors_map
    errors_map_ = sdsl::int_vector<>(
        mapping.size(), 0, sdsl::bits::hi(errors_table_.size() - 1) + 1);
    std::copy(mapping.begin(), mapping.end(), errors_map_.begin());
  }

  inline uint16_t get_error(size_t i) const {
    assert(i < errors_map_.size());
    assert(errors_map_[i] < errors_table_.size());
    return errors_table_[errors_map_[i]];
  }

  inline size_t size() const {
    return errors_map_.bit_size() / 8 + errors_table_.size() * sizeof(uint16_t);
  }

 private:
  std::vector<uint16_t> errors_table_;
  sdsl::int_vector<> errors_map_;
};

template <typename K>
class LecoCompression {
 public:
  LecoCompression(){};

  inline void Compress(const std::vector<K>& keys, int init_block_num) {
    point_num_ = keys.size();
    if (point_num_ <= 100) {
      points_ = keys;
      return;
    }
    memory_size_ = 0;

    if (point_num_ >= 10 * init_block_num) {
      block_num_ = init_block_num;
    } else {
      block_num_ = 10;
    }

    block_width_ = point_num_ / block_num_;
    block_num_ = point_num_ / block_width_;
    if (block_num_ * block_width_ < point_num_) {
      block_num_++;
    }  // handle with the last block, maybe < block_width_
    codec_.init(block_num_, block_width_);

    for (size_t i = 0; i < block_num_; i++) {
      int block_length = block_width_;
      if (i == block_num_ - 1) {
        block_length = point_num_ - (block_num_ - 1) * block_width_;
      }

      uint8_t* descriptor = (uint8_t*)malloc(block_length * sizeof(K) * 4);
      uint8_t* res = descriptor;
      res = codec_.encodeArray8_int(keys.data() + (i * block_width_),
                                    block_length, descriptor, i);
      uint32_t segment_size = res - descriptor;
      descriptor = (uint8_t*)realloc(descriptor, segment_size);
      block_start_vec_.push_back(descriptor);
      memory_size_ += segment_size;
    }
  }

  inline K decompress(size_t i) {
    if (point_num_ <= 100) {
      return points_[i];
    }
    return codec_.randomdecodeArray8(block_start_vec_[i / block_width_],
                                     i % block_width_, NULL, point_num_);
  }

  inline std::pair<size_t, K> LecoUpperBound(K key) {
    if (point_num_ <= 100) {
      auto it = std::upper_bound(points_.begin(), points_.end(), key);
      return {it - points_.begin(), *(--it)};
    }
    uint64_t s = 0, e = point_num_;
    K data_mid = 0, last_data = 0;
    while (s < e) {
      uint64_t mid = (s + e) >> 1;
      data_mid =
          codec_.randomdecodeArray8(block_start_vec_[mid / block_width_],
                                    mid % block_width_, NULL, point_num_);
      if (data_mid <= key)
        s = mid + 1;
      else
        e = mid;
    }
    last_data =
        codec_.randomdecodeArray8(block_start_vec_[(s - 1) / block_width_],
                                  (s - 1) % block_width_, NULL, point_num_);
    return {s, last_data};
  }

  inline size_t size() const { return memory_size_ + sizeof(size_t) * 4; }

  inline size_t keys_num() const { return point_num_; }

 private:
  Codecset::Leco_int<K> codec_;
  std::vector<uint8_t*> block_start_vec_;

  std::vector<K> points_;

  size_t point_num_;
  size_t block_num_;
  size_t memory_size_ = 0;
  size_t block_width_;
};

template <class K>
class LinearModel {
 public:
  int err_ = 0;
  float a_ = 0;   // slope
  double b_ = 0;  // intercept

 public:
  LinearModel() = default;
  LinearModel(int err, float a, double b) : err_(err), a_(a), b_(b) {}
  explicit LinearModel(const LinearModel& other)
      : err_(other.err_), a_(other.a_), b_(other.b_) {}

  LinearModel& operator=(const LinearModel& other) {
    a_ = other.a_;
    b_ = other.b_;
    err_ = other.err_;
    return *this;
  }

  inline int GetErr() const { return err_; }

  inline void UpdateErr(int err) {
    assert(err <= UINT16_MAX);
    if (err > err_) {
      err_ = err;
    }
  }

  inline int Predict(K key, int max_y) const {
    auto pred = static_cast<int>(a_ * static_cast<double>(key) + b_);
    pred = std::max(0, pred);
    pred = std::min(pred, max_y);
    return pred;
  }
};

template <typename K, typename V>
class LinearModelBuilder {
 public:
  //==============================================================
  // CONSTRUCTOR
  //==============================================================
  LinearModelBuilder(V max_data_y = 0, float avg_page = 1.05,
                     size_t record_per_page = 256)
      : count_(0),
        x_sum_(0),
        y_sum_(0),
        xx_sum_(0),
        xy_sum_(0),
        x_min_(std::numeric_limits<K>::max()),
        x_max_(std::numeric_limits<K>::lowest()),
        y_min_(std::numeric_limits<V>::max()),
        y_max_(std::numeric_limits<V>::lowest()),
        max_data_y_(max_data_y),
        acc_pages_(0),
        original_pages_(0),
        target_avg_page_(avg_page),
        record_per_page_(record_per_page) {
    assert(avg_page >= 1);
    assert(record_per_page >= 1);
  }

  explicit LinearModelBuilder(const LinearModelBuilder& other)
      : model_(other.model_),
        original_model_(other.original_model_),
        count_(other.count_),
        x_sum_(other.x_sum_),
        y_sum_(other.y_sum_),
        xx_sum_(other.xx_sum_),
        xy_sum_(other.xy_sum_),
        x_min_(other.x_min_),
        x_max_(other.x_max_),
        y_min_(other.y_min_),
        y_max_(other.y_max_),
        max_data_y_(other.max_data_y_),
        acc_pages_(other.acc_pages_),
        original_pages_(other.original_pages_),
        target_avg_page_(other.target_avg_page_),
        record_per_page_(other.record_per_page_) {}

  LinearModelBuilder& operator=(const LinearModelBuilder& other) {
    model_ = other.model_;
    original_model_ = other.original_model_;
    count_ = other.count_;
    x_sum_ = other.x_sum_;
    y_sum_ = other.y_sum_;
    xx_sum_ = other.xx_sum_;
    xy_sum_ = other.xy_sum_;
    x_min_ = other.x_min_;
    x_max_ = other.x_max_;
    y_min_ = other.y_min_;
    y_max_ = other.y_max_;
    max_data_y_ = other.max_data_y_;
    acc_pages_ = other.acc_pages_;
    original_pages_ = other.original_pages_;
    target_avg_page_ = other.target_avg_page_;
    record_per_page_ = other.record_per_page_;
    return *this;
  }

  //==============================================================
  // SPLIT PHASE
  //==============================================================
 public:
  template <typename RandomIt>
  inline std::pair<size_t, size_t> Add(RandomIt first, RandomIt last, int err,
                                       float a, double b) {
    auto it = first;
    while (it != last) {
      UpdateBuildInfo(it->first, it->second);
      it++;
    }
    acc_pages_ = 0;
    acc_pages_ = IncrementalCountPage(first, last, &model_);

    original_model_ = LinearModel<K>(err, a, b);
    CountPGMPage(first, last, false, &original_model_);
    return {original_pages_, acc_pages_};
  }

  //==============================================================
  // MERGE PHASE
  //==============================================================
 public:
  bool Merge(const LinearModelBuilder& other) {
    count_ += other.count_;

    x_sum_ += other.x_sum_;
    y_sum_ += other.y_sum_;
    xx_sum_ += other.xx_sum_;
    xy_sum_ += other.xy_sum_;

    x_min_ = std::min<K>(other.x_min_, x_min_);
    x_max_ = std::max<K>(other.x_max_, x_max_);
    y_min_ = std::min<V>(other.y_min_, y_min_);
    y_max_ = std::max<V>(other.y_max_, y_max_);

    if (other.model_.a_ == model_.a_ && other.model_.b_ == model_.b_) {
      // the two models are the same
      acc_pages_ += other.acc_pages_;
    } else {
      // train the new model
      auto params = Build(count_, x_sum_, y_sum_, xx_sum_, xy_sum_, x_min_,
                          x_max_, y_min_, y_max_);

      if (params.first == model_.a_ && params.second == model_.b_) {
        // use the incremental function to calculate the accurate pages
        return true;
      } else {
        model_.a_ = params.first;
        model_.b_ = params.second;
        acc_pages_ = 0;
      }
    }
    return false;
  }

  template <typename RandomIt>
  inline size_t CountAccuratePageSum(RandomIt begin, LinearModel<K>* model) {
    if (acc_pages_ == 0) {
      auto end_it = begin + y_max_ + 1;
      UpdateAccurateError(begin + y_min_, end_it, max_data_y_, model,
                          record_per_page_);
      for (auto it = begin + y_min_; it != end_it; it++) {
        acc_pages_ += GetSinglePageCnt(model, it->first);
      }
    }
    return acc_pages_;
  }

  template <typename RandomIt>
  inline size_t CountPGMPage(RandomIt begin, RandomIt end, bool set_acc_page,
                             LinearModel<K>* model) {
    UpdatePGMAccurateError(begin, end, begin->first, max_data_y_, model,
                           record_per_page_);
    size_t cnt = 0;
    for (auto it = begin; it != end; it++) {
      cnt += GetSinglePageCnt(model, it->first - begin->first);
    }
    if (set_acc_page) {
      acc_pages_ = cnt;
    } else {
      original_pages_ = cnt;
    }
    return cnt;
  }

  template <typename RandomIt>
  inline size_t IncrementalCountPage(RandomIt begin, RandomIt end,
                                     LinearModel<K>* model) {
    UpdateAccurateError(begin, end, max_data_y_, model, record_per_page_);
    for (auto it = begin; it != end; it++) {
      acc_pages_ += GetSinglePageCnt(model, it->first);
    }
    return acc_pages_;
  }

  template <typename RandomIt>
  static inline void UpdateAccurateError(RandomIt begin, RandomIt end, V max_y,
                                         LinearModel<K>* model,
                                         size_t record_per_page) {
    model->err_ = 0;
    for (auto it = begin; it != end; it++) {
      int p = model->Predict(it->first, max_y);
      int err = GetRangeError(p, it->second, record_per_page);
      model->UpdateErr(err);
    }
  }

  template <typename RandomIt>
  static inline void UpdatePGMAccurateError(RandomIt begin, RandomIt end,
                                            K min_x, V max_y,
                                            LinearModel<K>* model,
                                            size_t record_per_page) {
    model->err_ = 0;
    for (auto it = begin; it != end; it++) {
      int p = model->Predict(it->first - min_x, max_y);
      int err = GetRangeError(p, it->second, record_per_page);
      model->UpdateErr(err);
    }
  }

 private:
  inline void UpdateBuildInfo(K x, V y) {
    count_++;
    x_sum_ += static_cast<long double>(x);
    y_sum_ += static_cast<long double>(y);
    xx_sum_ += static_cast<long double>(x) * x;
    xy_sum_ += static_cast<long double>(x) * y;

    x_min_ = std::min<K>(x, x_min_);
    x_max_ = std::max<K>(x, x_max_);
    y_min_ = std::min<V>(y, y_min_);
    y_max_ = std::max<V>(y, y_max_);
    auto params = Build(count_, x_sum_, y_sum_, xx_sum_, xy_sum_, x_min_,
                        x_max_, y_min_, y_max_);
    model_.a_ = params.first;
    model_.b_ = params.second;
  }

  //==============================================================
  // COMMON PUBLIC FUNCTIONS
  //==============================================================
 public:
  K GetMinKey() const { return x_min_; }
  K GetMaxKey() const { return x_max_; }
  V GetMinY() const { return y_min_; }
  V GetMaxY() const { return y_max_; }
  V GetDataCnt() const { return count_; }
  size_t GetOriginalPageCnt() const { return original_pages_; }

  void ClearOriginalPageCnt() { original_pages_ = 0; }
  void ClearAccuratePageCnt() { acc_pages_ = 0; }

  //==============================================================
  // COMMON PRIVATE FUNCTIONS
  //==============================================================
 private:
  static int GetRangeError(V pred, V correct, int record_per_page) {
    size_t corr_pid = correct / record_per_page;
    V zero_left = corr_pid * record_per_page;  // start idx in this page
    V zero_right = (corr_pid + 1) * record_per_page - 1;  // last idx
    int range_error = 0;

    // extend item-level errors to page-level
    if (pred < zero_left) {
      range_error = zero_left - pred;
    } else if (pred > zero_right) {
      range_error = pred - zero_right;
    } else {
      range_error = 0;
    }
    return range_error;
  }

  std::pair<float, double> Build(int count, long double x_sum,
                                 long double y_sum, long double xx_sum,
                                 long double xy_sum, K x_min, K x_max, V y_min,
                                 V y_max) const {
    float a = 0;
    double b = 0;

    if (static_cast<long double>(count) * xx_sum - x_sum * x_sum == 0) {
      // all values in a bucket have the same key.
      b = static_cast<double>(y_sum) / count;
      return {a, b};
    }

    a = static_cast<float>(
        (static_cast<long double>(count) * xy_sum - x_sum * y_sum) /
        (static_cast<long double>(count) * xx_sum - x_sum * x_sum));
    b = static_cast<double>((y_sum - static_cast<long double>(a) * x_sum) /
                            count);

    // If floating point precision errors, fit spline
    if (a <= 0) {
      a = (y_max - y_min) / (x_max - x_min);
      b = y_min - static_cast<double>(x_min) * a;
    }
    return {a, b};
  }

  inline int GetSinglePageCnt(LinearModel<K>* model, K key) const {
    size_t pred = model->Predict(key, max_data_y_);
    size_t err = model->GetErr();
    size_t start = pred > err ? pred - err : 0;
    size_t end = pred + err < max_data_y_ ? pred + err : max_data_y_;
    int cnt = end / record_per_page_ - start / record_per_page_ + 1;
    return cnt;
  }

 public:
  LinearModel<K> model_;
  LinearModel<K> original_model_;

  int count_ = 0;

 private:
  long double x_sum_ = 0;
  long double y_sum_ = 0;
  long double xx_sum_ = 0;
  long double xy_sum_ = 0;
  K x_min_ = std::numeric_limits<K>::max();
  K x_max_ = std::numeric_limits<K>::lowest();
  V y_min_ = std::numeric_limits<V>::max();
  V y_max_ = std::numeric_limits<V>::lowest();

  V max_data_y_ = 0;
  size_t acc_pages_ = 0;
  size_t original_pages_ = 0;

  float target_avg_page_ = 0;
  size_t record_per_page_ = 0;
};

static float PrintReducedMemory(size_t original, size_t reduced) {
  float reduced_MiB = DI_MiB(original) - DI_MiB(reduced);
  std::cout << "the original size (MiB):" << DI_MiB(original);
  std::cout << ",\tnew size:" << DI_MiB(reduced) << ",\treduced:" << reduced_MiB
            << std::endl;
  return reduced_MiB;
}

static void PrintCurrTime() {
  time_t timep;
  time(&timep);
  char tmpTime[64];
  strftime(tmpTime, sizeof(tmpTime), "%Y-%m-%d %H:%M:%S", localtime(&timep));
  std::cout << "TEST time: " << tmpTime << std::endl;
}

template <typename K, typename V>
void GetDiskOrientedPGM(
    std::vector<std::pair<K, V>>& data, int epsilon, size_t record_per_page,
    V max_y,
    std::vector<typename pgm_page::PGMIndexPage<K>::CompressSegment>&
        pgm_segments,
    std::vector<std::pair<float, float>>& slope_range,
    std::vector<std::pair<long double, long double>>& intersec) {
  pgm_page::PGMIndexPage<K>::get_disk_segments(
      data.begin(), data.end(), epsilon, record_per_page, max_y, pgm_segments,
      slope_range, intersec);

#ifndef HYBRID_BENCHMARK
  std::vector<std::pair<float, float>> tmp_slope;
  std::vector<std::pair<long double, long double>> tmp_inter;
  std::vector<typename pgm_page::PGMIndexPage<K>::CompressSegment> origin_pgm;
  pgm_page::PGMIndexPage<K>::get_segments_compression(
      data.begin(), data.end(), epsilon, max_y, origin_pgm, tmp_slope,
      tmp_inter);
  PrintCurrTime();
  std::cout << ", after pgm splitting (on disk), #models:"
            << pgm_segments.size()
            << ",\tavg:" << data.size() * 1.0 / pgm_segments.size()
            << " records per model" << std::endl;
  std::cout << ", after pgm splitting (origin version), #models:"
            << origin_pgm.size()
            << ",\tavg:" << data.size() * 1.0 / origin_pgm.size()
            << " records per model,\tzero range reduces #model:"
            << origin_pgm.size() - pgm_segments.size() << ",\tpercentage: "
            << (origin_pgm.size() - pgm_segments.size()) * 1.0 /
                   origin_pgm.size()
            << std::endl;
#endif
}

}  // namespace compressed_disk_index

#endif  // !INDEXES_COMPRESSED_DISK_ORIENTED_INDEX_BASE_H_