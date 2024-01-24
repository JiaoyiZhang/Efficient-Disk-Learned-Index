#include <algorithm>
#include <iostream>
#include <map>
#include <set>
#include <unordered_set>
#include <vector>

#include "base.h"

namespace compressed_disk_index {

/**
 * PGM + error bound alignment + compression (without keys)
 */
template <typename K, typename V>
class DiskOrientedIndexV3 {
 public:
  DiskOrientedIndexV3(size_t record_per_page = 256)
      : min_key_(0), max_key_(0), max_y_(0), record_per_page_(record_per_page) {
    std::cout << "the model keys are uncompressed (the same as compressed pgm)"
              << std::endl;
  }

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
    error_ = pgm_epsilon + 1;
    std::vector<std::pair<float, float>> origin_slope_ranges;
    std::vector<std::pair<long double, long double>> origin_intersections;
    std::vector<typename pgm_page::PGMIndexPage<K>::CompressSegment>
        pgm_segments;

    GetDiskOrientedPGM(data, pgm_epsilon, record_per_page_, max_y_,
                       pgm_segments, origin_slope_ranges, origin_intersections);

    // store segments for the compression phase
    std::vector<Segments<K>> segments(pgm_segments.size() - 1);
    std::vector<K> model_keys(pgm_segments.size());
    for (size_t i = 0; i < segments.size(); i++) {
      segments[i] = {origin_slope_ranges[i], origin_intersections[i],
                     pgm_segments[i].key};
      model_keys[i] = pgm_segments[i].key;
    }
    model_keys[pgm_segments.size() - 1] = pgm_segments.back().key;
    const size_t seg_num = segments.size();

    // compress slopes
    auto intercepts = compressed_slopes.MergeCompress(segments);
    std::cout << "the slope-compression phase has been completed!" << std::endl;
    float reduced = 0, original = DI_MiB(seg_num * sizeof(K));
    std::cout << ",\tafter compressing the slopes:" << std::endl;
    original += DI_MiB(sizeof(float) * seg_num);
    reduced +=
        PrintReducedMemory(sizeof(float) * seg_num, compressed_slopes.size());

    // compress intercepts
    compressed_intercepts_.Compress(intercepts.begin(), intercepts.end());
    std::cout << ",\tafter compressing the intercepts:" << std::endl;
    original += DI_MiB(sizeof(uint64_t) * seg_num);
    reduced += PrintReducedMemory(sizeof(uint64_t) * seg_num,
                                  compressed_intercepts_.size());

    model_keys_ = model_keys;
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
    const size_t end = (pred + error_ > max_y_) ? max_y_ : (pred + error_);
    return SearchBound{begin, end + 1};
  }

  size_t GetModelNum() const { return model_keys_.size(); }

  size_t GetSize() const {
    return compressed_slopes.size() + compressed_intercepts_.size() +
           sizeof(uint16_t) + model_keys_.size() * sizeof(K);
  }

 private:
  inline std::pair<size_t, K> GetSegmentIndex(const K key) {
    size_t pos = std::upper_bound(model_keys_.begin(), model_keys_.end(), key) -
                 model_keys_.begin();
    return {pos - 1, model_keys_[pos - 1]};
  }

  inline size_t Predict(const K key, const size_t idx) const {
    int pred = compressed_slopes.get_slope(idx) * static_cast<double>(key) +
               compressed_intercepts_.get_intercept(idx);
    pred = pred < 0 ? 0 : pred;
    pred = static_cast<size_t>(pred) > max_y_ + 1 ? max_y_ + 1 : pred;
    return static_cast<size_t>(pred);
  }

 private:
  K min_key_;
  K max_key_;
  size_t max_y_;
  size_t record_per_page_;

  uint16_t error_;

  CompressedSlopes<K> compressed_slopes;
  CompressedIntercepts compressed_intercepts_;

  std::vector<K> model_keys_;
};

}  // namespace compressed_disk_index