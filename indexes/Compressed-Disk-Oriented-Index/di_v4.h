#include <algorithm>
#include <iostream>
#include <map>
#include <set>
#include <unordered_set>
#include <vector>

#include "base.h"

// #define INTERCEPT_USE_LECO

namespace compressed_disk_index {

/**
 * PGM + error bound alignment + compression
 */
template <typename K, typename V>
class DiskOrientedIndexV4 {
 public:
  DiskOrientedIndexV4(size_t record_per_page = 256)
      : min_key_(0), max_key_(0), max_y_(0), record_per_page_(record_per_page) {
#ifndef HYBRID_BENCHMARK
    std::cout << "use leco to compress the model keys" << std::endl;
#endif
  }

  void Build(std::vector<std::pair<K, V>>& data, float lambda = 1.05) {
#ifndef HYBRID_BENCHMARK
    PrintCurrTime();
#endif
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
#ifndef HYBRID_BENCHMARK
    std::cout << "pgm_epsilon:" << pgm_epsilon << std::endl;
#endif
    error_ = pgm_epsilon + 1;
#ifdef BREAKDOWN
    init_data_size = data.size();
    auto start = std::chrono::high_resolution_clock::now();
#endif
    std::vector<std::pair<float, float>> origin_slope_ranges;
    std::vector<std::pair<long double, long double>> origin_intersections;
    std::vector<typename pgm_page::PGMIndexPage<K>::CompressSegment>
        pgm_segments;
#ifdef BREAKDOWN
    auto end = std::chrono::high_resolution_clock::now();
    init_vector_lat +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
    start = std::chrono::high_resolution_clock::now();
#endif

    GetDiskOrientedPGM(data, pgm_epsilon, record_per_page_, max_y_,
                       pgm_segments, origin_slope_ranges, origin_intersections);
#ifdef BREAKDOWN
    end = std::chrono::high_resolution_clock::now();
    get_pgm_lat +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
    seg_size = pgm_segments.size();
    start = std::chrono::high_resolution_clock::now();
#endif

    // store segments for the compression phase
    std::vector<Segments<K>> segments(pgm_segments.size() - 1);
    std::vector<K> model_keys(pgm_segments.size());
#pragma omp parallel for num_threads(PARALLELISM)
    for (size_t i = 0; i < segments.size(); i++) {
      segments[i] = {origin_slope_ranges[i], origin_intersections[i],
                     pgm_segments[i].key};
      model_keys[i] = pgm_segments[i].key;
    }
    model_keys[pgm_segments.size() - 1] = pgm_segments.back().key;
    const size_t seg_num = segments.size();

#ifdef BREAKDOWN
    end = std::chrono::high_resolution_clock::now();
    store_seg_lat +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
    start = std::chrono::high_resolution_clock::now();
#endif
    // compress slopes
    auto intercepts = compressed_slopes.MergeCompress(segments);
#ifdef BREAKDOWN
    end = std::chrono::high_resolution_clock::now();
    cpr_slope_lat +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
    start = std::chrono::high_resolution_clock::now();
#endif
#ifndef HYBRID_BENCHMARK
    std::cout << "the slope-compression phase has been completed!" << std::endl;
    float reduced = 0, original = DI_MiB(seg_num * sizeof(K));
    std::cout << ",\tafter compressing the slopes:" << std::endl;
    original += DI_MiB(sizeof(float) * seg_num);
    reduced +=
        PrintReducedMemory(sizeof(float) * seg_num, compressed_slopes.size());
#endif

    // compress intercepts
#ifndef HYBRID_BENCHMARK
    std::vector<uint32_t> inter32(intercepts.begin(), intercepts.end());
    leco_intercepts_.Compress(inter32, 1000);
    std::cout << ",\tafter leco compressing the intercepts:" << std::endl;
    PrintReducedMemory(sizeof(uint64_t) * seg_num, leco_intercepts_.size());
#endif

    pgm_intercepts_.Compress(intercepts.begin(), intercepts.end());
#ifdef BREAKDOWN
    end = std::chrono::high_resolution_clock::now();
    cpr_intercept_lat +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
    start = std::chrono::high_resolution_clock::now();
#endif
#ifndef HYBRID_BENCHMARK
    std::cout << ",\tafter compressing the intercepts (PGM):" << std::endl;
    PrintReducedMemory(sizeof(uint64_t) * seg_num, pgm_intercepts_.size());
    if (pgm_intercepts_.size() < leco_intercepts_.size()) {
      std::cout << "LECO LOSE!" << std::endl;
    } else {
      std::cout << "LECO WIN!" << std::endl;
    }
    original += DI_MiB(sizeof(uint64_t) * seg_num);
#ifdef INTERCEPT_USE_LECO
    reduced +=
        PrintReducedMemory(sizeof(uint64_t) * seg_num, leco_intercepts_.size());
#else
    reduced +=
        PrintReducedMemory(sizeof(uint64_t) * seg_num, pgm_intercepts_.size());
#endif
#endif
    // compress keys
    compressed_keys.Compress(model_keys, 1000);
#ifdef BREAKDOWN
    end = std::chrono::high_resolution_clock::now();
    cpr_key_lat +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
#endif
#ifndef HYBRID_BENCHMARK
    std::cout << ",\tafter compressing the model keys:" << std::endl;
    reduced += PrintReducedMemory(sizeof(K) * seg_num, compressed_keys.size());
    std::cout << "the original memory:" << original
              << ",\tthe total reduced memory (MiB):" << reduced
              << ",\t%:" << reduced / original << std::endl;
#endif
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

  size_t GetModelNum() const { return compressed_keys.keys_num(); }

  size_t GetSize() const {
#ifdef INTERCEPT_USE_LECO
    return compressed_slopes.size() + leco_intercepts_.size() +
           sizeof(uint16_t) + compressed_keys.size();
#else
    return compressed_slopes.size() + pgm_intercepts_.size() +
           sizeof(uint16_t) + compressed_keys.size();
#endif
  }
#ifdef BREAKDOWN
  void PrintBreakdown() {
    std::cout << "di v4: ," << init_data_size << ",\t" << seg_size << ",\t"
              << init_vector_lat / 1e6 << ",\t" << get_pgm_lat / 1e6 << ",\t"
              << store_seg_lat / 1e6 << ",\t" << cpr_slope_lat / 1e6 << ",\t"
              << cpr_intercept_lat / 1e6 << ",\t" << cpr_key_lat / 1e6
              << std::endl;
  }
#endif

 private:
  inline std::pair<size_t, K> GetSegmentIndex(const K key) {
    auto res = compressed_keys.LecoUpperBound(key);
    assert(res.first >= 1);
    res.first -= 1;
    return res;
  }

  inline size_t Predict(const K key, const size_t idx) {
#ifdef INTERCEPT_USE_LECO
    int pred = compressed_slopes.get_slope(idx) * static_cast<double>(key) +
               leco_intercepts_.decompress(idx);
#else
    int pred = compressed_slopes.get_slope(idx) * static_cast<double>(key) +
               pgm_intercepts_.get_intercept(idx);
#endif
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
  LecoCompression<uint32_t> leco_intercepts_;
  CompressedIntercepts pgm_intercepts_;

  LecoCompression<K> compressed_keys;
#ifdef BREAKDOWN
  double init_vector_lat{0};
  double get_pgm_lat{0};
  double store_seg_lat{0};
  double cpr_slope_lat{0};
  double cpr_intercept_lat{0};
  double cpr_key_lat{0};
  size_t init_data_size = 0;
  size_t seg_size = 0;
#endif
};

}  // namespace compressed_disk_index