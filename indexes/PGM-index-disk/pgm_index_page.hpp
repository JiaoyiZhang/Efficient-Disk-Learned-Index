// This file is part of PGM-index <https://github.com/gvinciguerra/PGM-index>.
// Copyright (c) 2018 Giorgio Vinciguerra.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#include <assert.h>
#include <math.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

#include "./piecewise_linear_model.hpp"

namespace pgm_page {

#define PGM_SUB_EPS(x, epsilon) ((x) <= (epsilon) ? 0 : ((x) - (epsilon)))
#define PGM_ADD_EPS(x, epsilon, size) \
  ((x) + (epsilon) + 2 >= (size) ? (size) : (x) + (epsilon) + 2)

/**
 * A struct that stores the result of a query to a @ref PGMIndex, that is, a
 * range [@ref lo, @ref hi) centered around an approximate position @ref pos of
 * the sought key.
 */
struct ApproxPos {
  size_t pos;  ///< The approximate position of the key.
  size_t lo;   ///< The lower bound of the range.
  size_t hi;   ///< The upper bound of the range.
};

/**
 * A space-efficient index that enables fast search operations on a sorted
 * sequence of @c n numbers.
 *
 * A search returns a struct @ref ApproxPos containing an approximate position
 * of the sought key in the sequence and the bounds of a range of size
 * 2*Epsilon+1 where the sought key is guaranteed to be found if present. If the
 * key is not present, the range is guaranteed to contain a key that is not less
 * than (i.e. greater or equal to) the sought key, or @c n if no such key is
 * found. In the case of repeated keys, the index finds the position of the
 * first occurrence of a key.
 *
 * The @p Epsilon template parameter should be set according to the desired
 * space-time trade-off. A smaller value makes the estimation more precise and
 * the range smaller but at the cost of increased space usage.
 *
 * Internally the index uses a succinct piecewise linear mapping from keys to
 * their position in the sorted order. This mapping is represented as a sequence
 * of linear models (segments) which, if @p EpsilonRecursive is not zero, are
 * themselves recursively indexed by other piecewise linear mappings.
 *
 * @tparam K the type of the indexed keys
 * @tparam Epsilon controls the size of the returned search range
 * @tparam EpsilonRecursive controls the size of the search range in the
 * internal structure
 * @tparam Floating the floating-point type to use for slopes
 */
template <typename K, size_t EpsilonRecursive = 4, typename Floating = float>
class PGMIndexPage {
 public:
  struct CompressSegment;
  //==============================================================
  //  FOR COMPRESSION DISK-ORIENTED INDEX
  //==============================================================
  template <typename RandomIt>
  static void get_segments_compression(
      RandomIt first, RandomIt last, size_t epsilon, uint64_t max_y,
      std::vector<CompressSegment> &segments,
      std::vector<std::pair<float, float>> &slopes,
      std::vector<std::pair<long double, long double>> &intersections) {
    std::vector<size_t> levels_offsets;
    auto n = (size_t)std::distance(first, last);
    if (n == 0) return;

    levels_offsets.push_back(0);
    segments.reserve(n / (epsilon * epsilon));
    slopes.reserve(n / (epsilon * epsilon));
    intersections.reserve(n / (epsilon * epsilon));

    auto ignore_last =
        std::prev(last)->first ==
        std::numeric_limits<K>::max();  // max() is the sentinel value
    auto last_n = n - ignore_last;
    last -= ignore_last;

    auto build_level = [&](auto epsilon, auto in_fun, auto out_fun) {
      auto n_segments = pgm_page::internal::make_segmentation_par(
          last_n, epsilon, in_fun, out_fun);
      if (last_n > 1 && segments.back().slope == 0) {
        // Here we need to ensure that keys > *(last-1) are approximated to a
        // position == prev_level_size
        segments.emplace_back(std::prev(last)->first + 1, 0, last_n);
        segments.back().y = max_y;
        ++n_segments;
      }
      if (last_n == n - ignore_last) {
        segments.emplace_back(max_y);
      } else {
        segments.emplace_back(last_n);  // Add the sentinel segment
      }
      segments.back().y = max_y + 1;
      return n_segments;
    };

    // Build first level
    auto in_fun = [&](auto i) {
      auto x = first[i].first;
      // Here there is an adjustment for inputs with duplicate keys: at the end
      // of a run of duplicate keys equal to x=first[i] such that
      // x+1!=first[i+1], we map the values x+1,...,first[i+1]-1 to their
      // correct rank i
      auto flag = i > 0 && i + 1u < n && x == first[i - 1].first &&
                  x != first[i + 1].first && x + 1 != first[i + 1].first;
      return std::pair<K, size_t>(x + flag, first[i].second);
    };
    auto out_fun = [&](auto cs) {
      segments.emplace_back(cs);
      intersections.emplace_back(cs.get_intersection());
      slopes.emplace_back(cs.get_slope_range());
    };
    last_n = build_level(epsilon, in_fun, out_fun);
    levels_offsets.push_back(levels_offsets.back() + last_n + 1);
  }

  static pgm_page::internal::Y_Range GetRangeY(size_t y, size_t eps,
                                               size_t record_per_page,
                                               bool cross_page) {
    size_t corr_pid = y / record_per_page;
    size_t zero_left = corr_pid * record_per_page;  // start idx in this page
    size_t zero_right = (corr_pid + 1) * record_per_page - 1;  // last idx
    size_t y_low = zero_left >= eps ? zero_left - eps : 0;
    size_t y_high = zero_right <= std::numeric_limits<size_t>::max() - eps
                        ? zero_right + eps
                        : std::numeric_limits<size_t>::max();
    if (!cross_page && eps < record_per_page / 4 - 1) {
      // excessive merging -> avg page becomes large
      y_low = zero_left + 2 * eps;
      y_high = zero_right - 2 * eps;
    }
    y_low = std::min(y_low, y);
    y_high = std::max(y_high, y);
    return {y, y_low, y_high};
  }

  template <typename RandomIt>
  static void get_disk_segments(
      RandomIt first, RandomIt last, size_t epsilon, size_t record_per_page,
      uint64_t max_y, std::vector<CompressSegment> &segments,
      std::vector<std::pair<float, float>> &slopes,
      std::vector<std::pair<long double, long double>> &intersections) {
    std::vector<size_t> levels_offsets;
    auto n = (size_t)std::distance(first, last);
    if (n == 0) return;

    levels_offsets.push_back(0);
    segments.reserve(n / (epsilon * epsilon));
    slopes.reserve(n / (epsilon * epsilon));
    intersections.reserve(n / (epsilon * epsilon));

    auto ignore_last =
        std::prev(last)->first ==
        std::numeric_limits<K>::max();  // max() is the sentinel value
    auto last_n = n - ignore_last;
    last -= ignore_last;

    auto build_level = [&](auto epsilon, auto in_fun, auto out_fun) {
      auto n_segments = pgm_page::internal::make_segmentation_range_par(
          last_n, epsilon, in_fun, out_fun);
      if (last_n > 1 && segments.back().slope == 0) {
        // Here we need to ensure that keys > *(last-1) are approximated to a
        // position == prev_level_size
        segments.emplace_back(std::prev(last)->first + 1, 0, last_n);
        segments.back().y = max_y;
        ++n_segments;
      }
      if (last_n == n - ignore_last) {
        segments.emplace_back(max_y);
      } else {
        segments.emplace_back(last_n);  // Add the sentinel segment
      }
      segments.back().y = max_y + 1;
      return n_segments;
    };

    size_t cross_num = (2 * epsilon + 1) * 1.0 / record_per_page * n;
    size_t cross_gap = n / 2 > cross_num ? n / 2 / cross_num : 1.0;

    // Build first level
    auto in_fun = [&](auto i) {
      auto x = first[i].first;
      // Here there is an adjustment for inputs with duplicate keys: at the end
      // of a run of duplicate keys equal to x=first[i] such that
      // x+1!=first[i+1], we map the values x+1,...,first[i+1]-1 to their
      // correct rank i
      auto flag = i > 0 && i + 1u < n && x == first[i - 1].first &&
                  x != first[i + 1].first && x + 1 != first[i + 1].first;
      bool is_cross_page = false;
      if (i % cross_gap == 0) {
        is_cross_page = true;
      }
      auto y =
          GetRangeY(first[i].second, epsilon, record_per_page, is_cross_page);
      // return std::pair<K, size_t>(x + flag, first[i].second);
      return std::pair<K, pgm_page::internal::Y_Range>(x + flag, y);
    };
    auto out_fun = [&](auto cs) {
      segments.emplace_back(cs);
      intersections.emplace_back(cs.get_intersection());
      slopes.emplace_back(cs.get_slope_range());
    };
    last_n = build_level(epsilon, in_fun, out_fun);
    levels_offsets.push_back(levels_offsets.back() + last_n + 1);
  }

  template <typename RandomIt>
  static std::pair<Floating, Floating> get_slope_range(
      RandomIt first, RandomIt last, size_t &epsilon, size_t max_epsilon,
      size_t record_per_page, bool linear_search,
      std::pair<long double, long double> &intersection) {
    auto n = (size_t)std::distance(first, last);
    std::pair<Floating, Floating> slope_range;

    auto build_level = [&](auto epsilon, auto in_fun, auto out_fun) {
      auto eps = epsilon;
      int n_segments;
      if (!linear_search) {
        // use the binary search to find the minimum epsilon
        size_t l = epsilon, r = max_epsilon;
        while (l < r) {
          eps = (l + r) >> 1;
          n_segments = pgm_page::internal::make_segmentation_disk(
              n, eps, in_fun, out_fun);

          if (n_segments > 1)
            l = eps + 1;
          else
            r = eps;
        }
        eps = l;
      }
      // use the linear search to find the minimum epsilon
      n_segments =
          pgm_page::internal::make_segmentation_disk(n, eps, in_fun, out_fun);
      while (n_segments > 1) {
        eps++;
        n_segments =
            pgm_page::internal::make_segmentation_disk(n, eps, in_fun, out_fun);
      }
      assert(n_segments <= 1);
      return eps;
    };

    // Build first level
    auto in_fun = [&](auto i) {
      auto x = first[i].first;
      // Here there is an adjustment for inputs with duplicate keys: at the end
      // of a run of duplicate keys equal to x=first[i] such that
      // x+1!=first[i+1], we map the values x+1,...,first[i+1]-1 to their
      // correct rank i
      auto flag = i > 0 && i + 1u < n && x == first[i - 1].first &&
                  x != first[i + 1].first && x + 1 != first[i + 1].first;
      auto y = GetRangeY(first[i].second, max_epsilon, record_per_page, true);
      return std::pair<K, pgm_page::internal::Y_Range>(x + flag, y);
      // return std::pair<K, size_t>(x + flag, first[i].second);
    };
    auto out_fun = [&](auto cs) {
      slope_range = cs.get_slope_range();
      intersection = cs.get_intersection();
    };
    epsilon = build_level(epsilon, in_fun, out_fun);
    return slope_range;
  }
  //==============================================================
  //  END
  //==============================================================

 protected:
  template <typename, size_t, size_t, uint8_t, typename>
  friend class BucketingPGMIndex;

  template <typename, size_t, typename>
  friend class EliasFanoPGMIndex;

  struct Segment;

  size_t n;        ///< The number of elements this index was built on.
  uint64_t max_y;  ///< The max value.
  K first_key;     ///< The smallest element.
  std::vector<Segment> segments;       ///< The segments composing the index.
  std::vector<size_t> levels_offsets;  ///< The starting position of each level
                                       ///< in segments[], in reverse order.

  template <typename RandomIt>
  static void build(RandomIt first, RandomIt last, size_t epsilon,
                    uint64_t max_y, size_t epsilon_recursive,
                    std::vector<Segment> &segments,
                    std::vector<size_t> &levels_offsets) {
    auto n = (size_t)std::distance(first, last);
    if (n == 0) return;

    levels_offsets.push_back(0);
    segments.reserve(n / (epsilon * epsilon));

    auto ignore_last =
        std::prev(last)->first ==
        std::numeric_limits<K>::max();  // max() is the sentinel value
    auto last_n = n - ignore_last;
    last -= ignore_last;

    auto build_level = [&](auto epsilon, auto in_fun, auto out_fun) {
      auto n_segments = pgm_page::internal::make_segmentation_par(
          last_n, epsilon, in_fun, out_fun);
      if (last_n > 1 && segments.back().slope == 0) {
        // Here we need to ensure that keys > *(last-1) are approximated to a
        // position == prev_level_size
        segments.emplace_back(std::prev(last)->first + 1, 0, last_n);
        ++n_segments;
      }
      if (last_n == n - ignore_last) {
        segments.emplace_back(max_y);
      } else {
        segments.emplace_back(last_n);  // Add the sentinel segment
      }
      return n_segments;
    };

    // Build first level
    auto in_fun = [&](auto i) {
      auto x = first[i].first;
      // Here there is an adjustment for inputs with duplicate keys: at the end
      // of a run of duplicate keys equal to x=first[i] such that
      // x+1!=first[i+1], we map the values x+1,...,first[i+1]-1 to their
      // correct rank i
      auto flag = i > 0 && i + 1u < n && x == first[i - 1].first &&
                  x != first[i + 1].first && x + 1 != first[i + 1].first;
      return std::pair<K, size_t>(x + flag, first[i].second);
    };
    auto out_fun = [&](auto cs) { segments.emplace_back(cs); };
    last_n = build_level(epsilon, in_fun, out_fun);
    levels_offsets.push_back(levels_offsets.back() + last_n + 1);

    // Build upper levels
    while (epsilon_recursive && last_n > 1) {
      auto offset = levels_offsets[levels_offsets.size() - 2];
      auto in_fun_rec = [&](auto i) {
        return std::pair<K, size_t>(segments[offset + i].key, i);
      };
      last_n = build_level(epsilon_recursive, in_fun_rec, out_fun);
      levels_offsets.push_back(levels_offsets.back() + last_n + 1);
    }
  }

  /**
   * Returns the segment responsible for a given key, that is, the rightmost
   * segment having key <= the sought key.
   * @param key the value of the element to search for
   * @return an iterator to the segment responsible for the given key
   */
  auto segment_for_key(const K &key) const {
    if constexpr (EpsilonRecursive == 0) {
      return std::prev(std::upper_bound(
          segments.begin(), segments.begin() + segments_count(), key));
    }

    auto it = segments.begin() + *(levels_offsets.end() - 2);
    for (auto l = int(height()) - 2; l >= 0; --l) {
      auto level_begin = segments.begin() + levels_offsets[l];
      auto pos = std::min<size_t>((*it)(key), std::next(it)->intercept);
      auto lo = level_begin + PGM_SUB_EPS(pos, EpsilonRecursive + 1);

      static constexpr size_t linear_search_threshold =
          8 * 64 / sizeof(Segment);
      if constexpr (EpsilonRecursive <= linear_search_threshold) {
        for (; std::next(lo)->key <= key; ++lo) continue;
        it = lo;
      } else {
        auto level_size = levels_offsets[l + 1] - levels_offsets[l] - 1;
        auto hi = level_begin + PGM_ADD_EPS(pos, EpsilonRecursive, level_size);
        it = std::prev(std::upper_bound(lo, hi, key));
      }
    }
    return it;
  }

 public:
  size_t epsilon_value;

  /**
   * Constructs an empty index.
   */
  PGMIndexPage() = default;

  /**
   * Constructs the index on the given sorted vector.
   * @param data the vector of keys to be indexed, must be sorted
   */
  explicit PGMIndexPage(const std::vector<std::pair<K, size_t>> &data,
                        size_t eps = 64)
      : PGMIndexPage(data.begin(), data.end(), eps) {}

  /**
   * Constructs the index on the sorted keys in the range [first, last).
   * @param first, last the range containing the sorted keys to be indexed
   */
  template <typename RandomIt>
  PGMIndexPage(RandomIt first, RandomIt last, size_t eps = 64)
      : n(std::distance(first, last)),
        first_key(n ? first->first : K(0)),
        segments(),
        levels_offsets() {
    epsilon_value = eps;
    max_y = (last - 1)->second;
    build(first, last, eps, max_y, EpsilonRecursive, segments, levels_offsets);
  }

  /**
   * Returns the approximate position and the range where @p key can be found.
   * @param key the value of the element to search for
   * @return a struct with the approximate position and bounds of the range
   */
  ApproxPos search(const K &key) const {
    auto k = std::max(first_key, key);
    auto it = segment_for_key(k);
    auto pos = std::min<size_t>((*it)(k), std::next(it)->intercept);
    auto lo = PGM_SUB_EPS(pos, epsilon_value);
    auto hi = PGM_ADD_EPS(pos, epsilon_value, (max_y + 1));
    return {pos, lo, hi};
  }

  /**
   * Returns the number of segments in the last level of the index.
   * @return the number of segments
   */
  size_t segments_count() const {
    return segments.empty() ? 0 : levels_offsets[1] - 1;
  }

  /**
   * Returns the number of levels of the index.
   * @return the number of levels of the index
   */
  size_t height() const { return levels_offsets.size() - 1; }

  /**
   * Returns the size of the index in bytes.
   * @return the size of the index in bytes
   */
  size_t size_in_bytes() const {
    return segments.size() * sizeof(Segment) +
           levels_offsets.size() * sizeof(size_t);
  }
};

#pragma pack(push, 1)

template <typename K, size_t EpsilonRecursive, typename Floating>
struct PGMIndexPage<K, EpsilonRecursive, Floating>::Segment {
  K key;              ///< The first key that the segment indexes.
  Floating slope;     ///< The slope of the segment.
  int64_t intercept;  ///< The intercept of the segment.
  // int32_t intercept;  ///< The intercept of the segment.

  Segment() = default;

  Segment(K key, Floating slope, int32_t intercept)
      : key(key), slope(slope), intercept(intercept){};

  explicit Segment(size_t n)
      : key(std::numeric_limits<K>::max()), slope(), intercept(n){};

  explicit Segment(
      const typename pgm_page::internal::OptimalPiecewiseLinearModel<
          K, size_t>::CanonicalSegment &cs)
      : key(cs.get_first_x()) {
    auto [cs_slope, cs_intercept] = cs.get_floating_point_segment(key);
    if (cs_intercept > std::numeric_limits<decltype(intercept)>::max())
      throw std::overflow_error(
          "Change the type of Segment::intercept to int64");
    slope = cs_slope;
    intercept = cs_intercept;
  }

  friend inline bool operator<(const Segment &s, const K &k) {
    return s.key < k;
  }
  friend inline bool operator<(const K &k, const Segment &s) {
    return k < s.key;
  }
  friend inline bool operator<(const Segment &s, const Segment &t) {
    return s.key < t.key;
  }

  operator K() { return key; };

  /**
   * Returns the approximate position of the specified key.
   * @param k the key whose position must be approximated
   * @return the approximate position of the specified key
   */
  inline size_t operator()(const K &k) const {
    auto pos = int64_t(slope * (k - key)) + intercept;
    return pos > 0 ? size_t(pos) : 0ull;
  }
};

//==============================================================
//  FOR COMPRESSION DISK-ORIENTED INDEX
//==============================================================
template <typename K, size_t EpsilonRecursive, typename Floating>
struct PGMIndexPage<K, EpsilonRecursive, Floating>::CompressSegment {
  K key;              ///< The first key that the segment indexes.
  Floating slope;     ///< The slope of the segment.
  int64_t intercept;  ///< The intercept of the segment.
  size_t y;

  CompressSegment() = default;

  CompressSegment(K key, Floating slope, int32_t intercept)
      : key(key), slope(slope), intercept(intercept){};

  explicit CompressSegment(size_t n)
      : key(std::numeric_limits<K>::max()), slope(), intercept(n){};

  explicit CompressSegment(
      const typename pgm_page::internal::OptimalPiecewiseLinearModel<
          K, size_t>::CanonicalSegment &cs)
      : key(cs.get_first_x()), y(cs.get_first_y()) {
    auto [cs_slope, cs_intercept] = cs.get_floating_point_segment(key);
    slope = cs_slope;
    intercept = cs_intercept;
  }

  friend inline bool operator<(const CompressSegment &s, const K &k) {
    return s.key < k;
  }
  friend inline bool operator<(const K &k, const CompressSegment &s) {
    return k < s.key;
  }
  friend inline bool operator<(const CompressSegment &s,
                               const CompressSegment &t) {
    return s.key < t.key;
  }

  operator K() { return key; };

  /**
   * Returns the approximate position of the specified key.
   * @param k the key whose position must be approximated
   * @return the approximate position of the specified key
   */
  inline size_t operator()(const K &k) const {
    auto pos = int64_t(slope * (k - key)) + intercept;
    return pos > 0 ? size_t(pos) : 0ull;
  }
};
//==============================================================
//  END
//==============================================================

#pragma pack(pop)

}  // namespace pgm_page