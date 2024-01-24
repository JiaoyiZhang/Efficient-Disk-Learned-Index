#include <algorithm>
#include <iostream>
#include <map>
#include <set>
#include <unordered_set>
#include <vector>

#include "base.h"

namespace compressed_disk_index {
/**
 * PGM + error bound alignment
 */
template <typename K, typename V>
class DiskOrientedIndexV1 {
 public:
  DiskOrientedIndexV1(size_t record_per_page = 256)
      : min_key_(0),
        max_key_(0),
        max_y_(0),
        record_per_page_(record_per_page),
        error_(1) {}

  void Build(std::vector<std::pair<K, V>>& data, float lambda = 1.05) {
    PrintCurrTime();
    min_key_ = data[0].first;
    max_key_ = data.back().first;
    max_y_ = data.back().second;

    // Split
    size_t pgm_epsilon = (lambda - 1) * record_per_page_ / 2 - 1;
    // for experiments
    if (pgm_epsilon == 5) {
      pgm_epsilon = 7;
    }
    if (pgm_epsilon == 14) {
      pgm_epsilon = 15;
    }
    std::cout << "pgm_epsilon:" << pgm_epsilon << std::endl;
    error_ = pgm_epsilon;
    std::vector<std::pair<float, float>> origin_slope_ranges;
    std::vector<std::pair<long double, long double>> origin_intersections;
    std::vector<typename pgm_page::PGMIndexPage<K>::CompressSegment>
        pgm_segments;

    GetDiskOrientedPGM(data, pgm_epsilon, record_per_page_, max_y_,
                       pgm_segments, origin_slope_ranges, origin_intersections);

    model_keys_.reserve(pgm_segments.size());
    a_.reserve(pgm_segments.size());
    b_.reserve(pgm_segments.size());
    for (size_t i = 0; i < pgm_segments.size(); i++) {
      model_keys_.push_back(pgm_segments[i].key);
      a_.push_back(pgm_segments[i].slope);
      b_.push_back(pgm_segments[i].intercept);
    }
  }

  SearchBound GetSearchBound(const K key) {
    if (key <= min_key_) {
      return {0, 1};
    } else if (key >= max_key_) {
      return {max_y_, max_y_ + 1};
    }
    const auto res = GetSegmentIndex(key);

    const size_t pred = Predict(key - res.second, res.first);
    const size_t begin = (pred < error_) ? 0 : (pred - error_);
    const size_t end =
        (pred + error_ + 1 > max_y_) ? max_y_ : (pred + error_ + 1);
    return SearchBound{begin, end + 1};
  }

  size_t GetModelNum() const { return a_.size(); }

  size_t GetSize() const {
    return a_.size() * (sizeof(K) + sizeof(float) + sizeof(double)) +
           sizeof(K) + sizeof(error_);
  }

 private:
  inline std::pair<size_t, K> GetSegmentIndex(const K key) const {
    size_t pos = std::upper_bound(model_keys_.begin(), model_keys_.end(), key) -
                 model_keys_.begin();
    return {pos - 1, model_keys_[pos - 1]};
  }

  inline size_t Predict(const K key, const size_t idx) const {
    int pred = a_[idx] * static_cast<double>(key) + b_[idx];
    pred = pred < 0 ? 0 : pred;
    pred = static_cast<size_t>(pred) > max_y_ + 1 ? max_y_ + 1 : pred;
    return static_cast<size_t>(pred);
  }

 private:
  K min_key_;
  K max_key_;
  size_t max_y_;
  size_t record_per_page_;

  std::vector<K> model_keys_;
  uint16_t error_;
  std::vector<float> a_;
  std::vector<double> b_;
};

}  // namespace compressed_disk_index