/**
 * @file structures.h
 * @author Making In-Memory Learned Indexes Efficient on Disk
 * @brief Structures for our microbenchmark
 * @version 0.1
 * @date 2022-10-06
 *
 * @copyright Copyright (c) 2022
 *
 */
#ifndef EXPERIMENTS_STRUCTURES_H_
#define EXPERIMENTS_STRUCTURES_H_

#include <unistd.h>

#include <fstream>
#include <iostream>
#include <map>
#include <random>

#include "macro.h"

struct SearchRange {
  uint64_t start;
  uint64_t stop;  // exclusive
};

struct FetchRange {
  uint64_t fid_start;
  uint64_t pid_start;
  uint64_t fid_end;
  uint64_t pid_end;
};

struct PageStats {
  float avg_dist;
  float avg_page;
  float avg_mid;
};

template <typename Value>
struct LookupInfo {
  uint64_t total_len;
  uint64_t max_len;
  Value actual_res;
};

template <typename Key>
struct ResultInfo {
  Key res;
  uint64_t fetch_page_num;
  uint64_t max_search_range;
  uint64_t total_search_range;  // in bytes
  uint64_t total_io;
  uint64_t ops;
  uint64_t latency_sum;
  uint64_t index_predict_time;
  uint64_t cpu_time;
  uint64_t io_time;

  ResultInfo() {
    res = 0;
    fetch_page_num = 0;
    max_search_range = 0;
    total_search_range = 0;
    total_io = 0;
    ops = 0;
    latency_sum = 0;
    index_predict_time = 0;
    cpu_time = 0;
    io_time = 0;
  }
};

enum FindStatus { kEqualToKey, kLessThanKey, kGreaterThanKey };

enum FetchStrategy {
  kStartWorstCase,
  kStartOneByOne,
  kMiddleWorstCase,
  kMiddleOneByOne,
  kLecoFetch
};

enum IndexName {
  kPGMIndex,
  kPGMIndexPage,
  kRadixSpline,
  kPGM_PG,
  kRS_PG,
  kRS_DISK,
  kCompressedPGM,
  kBinarySearch,
  kDIV1,
  kDIV3,
  kDIV4,
  kLecoZonemap,
  KLecoPage,
  kDISK
};

struct CompressedBlockRange {
  uint64_t idx;
  uint64_t block_sid;
  uint64_t block_eid;  // inclusive
  size_t offset;
  size_t block_bytes;
};

class CompressedBlockSize {
 public:
  CompressedBlockSize() {}
  CompressedBlockSize(const CompressedBlockSize& other)
      : byte_offset_(other.byte_offset_),
        key_num_(other.key_num_),
        mean_(other.mean_),
        stddev_(other.stddev_) {}
  CompressedBlockSize& operator=(const CompressedBlockSize& other) {
    if (this != &other) {
      byte_offset_ = other.byte_offset_;
      key_num_ = other.key_num_;
      mean_ = other.mean_;
      stddev_ = other.stddev_;
    }
    return *this;
  }

  explicit CompressedBlockSize(size_t block_bytes, size_t record_bytes,
                               size_t dataset_bytes, size_t key_bytes) {
    stddev_ = 1;

#if ALIGNED_COMPRESSION == 0
    mean_ = block_bytes / record_bytes;
    std::random_device rd;
    std::normal_distribution<double> dis(mean_, stddev_);
    size_t stored_num = 0, sum = 0;
    auto data_num = dataset_bytes / record_bytes;
    while (stored_num < data_num) {
      size_t bytes = static_cast<size_t>(dis(rd)) * record_bytes;
      if (bytes < 1) {
        bytes = 1;
      } else if (bytes / record_bytes + stored_num > data_num) {
        bytes = (data_num - stored_num) * record_bytes;
      }
      stored_num += bytes / record_bytes;
      key_num_.push_back(stored_num);
      bytes =
          std::ceil(static_cast<float>(bytes) / getpagesize()) * getpagesize();
      sum += bytes;
      byte_offset_.push_back(sum);
    }
#else
    mean_ = MAX_PAYLOAD_LENGTH / 2 / key_bytes;
    std::random_device rd;
    std::normal_distribution<double> dis(mean_, stddev_);
    size_t stored_num = 0, sum = 0;
    auto data_num = dataset_bytes / record_bytes;
    auto records_per_block = block_bytes / record_bytes;
    while (stored_num < data_num) {
      int tmp = std::max(dis(rd), 0.0);
      size_t tmp_bytes = tmp * key_bytes + key_bytes;
      if (tmp_bytes < key_bytes) {
        tmp_bytes = key_bytes;
      } else if (tmp_bytes > MAX_PAYLOAD_LENGTH + key_bytes) {
        tmp_bytes = MAX_PAYLOAD_LENGTH + key_bytes;
      }
      stored_num += records_per_block;
      key_num_.push_back(tmp_bytes);
      sum += tmp_bytes * records_per_block;
      byte_offset_.push_back(sum);
    }
#endif
    std::cout << "generate " << byte_offset_.size()
              << " pages with different block sizes,\t the mean is:" << mean_
              << ",\t the standard  deviation is:" << stddev_
              << ",\tthe scaling factor is:" << record_bytes << std::endl;
  }

  std::pair<CompressedBlockRange, uint64_t> GetBlockRange(
      size_t offset, size_t end_offset) const {
    auto it =
        std::lower_bound(byte_offset_.begin(), byte_offset_.end(), offset);
    CompressedBlockRange range;
    if (it == byte_offset_.end()) {
      range.block_sid = UINT64_MAX;
      return {range, UINT64_MAX};
    }
    size_t bytes = *it;
    range.idx = it - byte_offset_.begin();
    if (range.idx == 0) {
      range.block_sid = 0;
      range.block_bytes = bytes;
      range.offset = 0;
    } else {
      it--;
      range.offset = (*it);
      range.block_sid = (*it) / getpagesize();
      range.block_bytes = bytes - (*it);
    }
    range.block_eid = (bytes - 1) / getpagesize();
    auto end_range_bytes = range.block_bytes;
    while (*it <= end_offset - 1) {
      it++;
    }
    end_range_bytes = *it;
    return {range, end_range_bytes};
  }

  CompressedBlockRange GetNextBlockRange(uint64_t next_idx) const {
    auto it = byte_offset_.begin() + next_idx;
    CompressedBlockRange range;
    if (it == byte_offset_.end()) {
      range.block_sid = UINT64_MAX;
      return range;
    }
    size_t bytes = *it;
    range.idx = it - byte_offset_.begin();
    if (range.idx == 0) {
      range.block_sid = 0;
      range.block_bytes = bytes;
      range.offset = 0;
    } else {
      it--;
      range.offset = (*it);
      range.block_sid = (*it) / getpagesize();
      range.block_bytes = bytes - (*it);
    }
    range.block_eid = (bytes - 1) / getpagesize();
    return range;
  }

  int GetMean() { return mean_; }

  std::vector<size_t> byte_offset_;  // the byte offset of each block

  /**
   * @brief Used for initialization.
   * In aligned-compression mode, each element is the accumulated number of
   * stored records. In sequential-compression mode, each element is the bytes
   * of the records in each block.
   */
  std::vector<size_t> key_num_;

 private:
  int mean_;
  int stddev_;
};

template <typename Key>
class Params {
 public:
  bool is_on_disk_;
  bool is_compression_mode_;

  std::string dataset_filename_;
  std::string data_dir_;  // useless in-memory mode
  std::map<int, int> open_files;
  Key* read_buf_;

  size_t payload_bytes_;
  size_t record_bytes_;
  size_t dataset_bytes_;  // useless in normal and in-memory mode
  size_t page_bytes_;     // useless in compression and in-memory mode
  size_t file_bytes_;     // useless in compression and in-memory mode

  /**
   * @brief Useless in aligned-compression and in-memory mode. In the
   * sequential-compression mode, the page_bytes and record_bytes is used to
   * determine the number of records in each block.
   */
  uint64_t record_num_per_page_;
  uint64_t record_num_per_file_;  // useless in compression and in-memory mode
  uint64_t page_num_per_file_;    // useless in compression and in-memory mode

  FetchStrategy fetch_strategy_;  // useless in compression and in-memory mode
  uint64_t pred_granularity_;     // useless in compression mode

  CompressedBlockSize comp_block_bytes;  // only for compression mode

  Params() { payload_bytes_ = 0; }

  Params(char* argv[], size_t dataset_size) {
    char* endptr;
    is_on_disk_ = strtoul(argv[1], &endptr, 10);
    dataset_filename_ = argv[2];
    payload_bytes_ = strtoul(argv[3], &endptr, 10);
    pred_granularity_ = strtoul(argv[4], &endptr, 10);
    record_bytes_ = payload_bytes_ + sizeof(Key);
    dataset_bytes_ = dataset_size * record_bytes_;
    is_compression_mode_ = false;
    fetch_strategy_ = kStartWorstCase;
    page_bytes_ = 4 * 1024;
    if (is_on_disk_) {
      data_dir_ = argv[9];
      is_compression_mode_ = strtoul(argv[11], &endptr, 10);
      read_buf_ = reinterpret_cast<Key*>(
          aligned_alloc(page_bytes_, page_bytes_ * ALLOCATED_BUF_SIZE));
      if (read_buf_ == nullptr) {
        throw std::runtime_error("read_buf_ memalign error in Params()");
      }

      const uint64_t kFileSize = strtoul(argv[10], &endptr, 10);
      const uint64_t kPageSize = strtoul(argv[12], &endptr, 10);
      int strategy = strtoul(argv[13], &endptr, 10);
      if (strategy <= kLecoFetch) {
        fetch_strategy_ = FetchStrategy(strategy);
      } else {
        throw std::runtime_error(
            "The parameter of the fetching strategy is invalid!");
      }

      file_bytes_ = kFileSize * 1024 * 1024;
      page_bytes_ = kPageSize * 1024;

      if (file_bytes_ % page_bytes_ != 0 || page_bytes_ % getpagesize() != 0 ||
          payload_bytes_ % sizeof(Key) != 0 ||
          page_bytes_ % record_bytes_ != 0 || pred_granularity_ < 1) {
        throw std::runtime_error("The parameters are invalid!");
      }
      if (is_compression_mode_) {
        comp_block_bytes = CompressedBlockSize(page_bytes_, record_bytes_,
                                               dataset_bytes_, sizeof(Key));
        if (file_bytes_ < comp_block_bytes.byte_offset_.back()) {
          file_bytes_ = comp_block_bytes.byte_offset_.back();
        }
      }
    }
    record_num_per_page_ = page_bytes_ / record_bytes_;
    record_num_per_file_ = file_bytes_ / record_bytes_;
    page_num_per_file_ = file_bytes_ / page_bytes_;
  }

  Params(const Params<Key>& other)
      : is_on_disk_(other.is_on_disk_),
        is_compression_mode_(other.is_compression_mode_),
        dataset_filename_(other.dataset_filename_),
        data_dir_(other.data_dir_),
        open_files(other.open_files),
        read_buf_(other.read_buf_),
        payload_bytes_(other.payload_bytes_),
        record_bytes_(other.record_bytes_),
        dataset_bytes_(other.dataset_bytes_),
        page_bytes_(other.page_bytes_),
        file_bytes_(other.file_bytes_),
        record_num_per_page_(other.record_num_per_page_),
        record_num_per_file_(other.record_num_per_file_),
        page_num_per_file_(other.page_num_per_file_),
        fetch_strategy_(other.fetch_strategy_),
        pred_granularity_(other.pred_granularity_),
        comp_block_bytes(other.comp_block_bytes) {}

  Params& operator=(const Params<Key>& other) {
    if (this != &other) {
      is_on_disk_ = other.is_on_disk_;
      is_compression_mode_ = other.is_compression_mode_;
      dataset_filename_ = other.dataset_filename_;
      data_dir_ = other.data_dir_;
      open_files = other.open_files;
      // read_buf_ = other.read_buf_;
      payload_bytes_ = other.payload_bytes_;
      record_bytes_ = other.record_bytes_;
      dataset_bytes_ = other.dataset_bytes_;
      page_bytes_ = other.page_bytes_;
      file_bytes_ = other.file_bytes_;
      record_num_per_page_ = other.record_num_per_page_;
      record_num_per_file_ = other.record_num_per_file_;
      page_num_per_file_ = other.page_num_per_file_;
      fetch_strategy_ = other.fetch_strategy_;
      pred_granularity_ = other.pred_granularity_;
      comp_block_bytes = other.comp_block_bytes;
    }
    return *this;
  }

  void alloc() {
    read_buf_ = reinterpret_cast<Key*>(
        aligned_alloc(page_bytes_, page_bytes_ * ALLOCATED_BUF_SIZE));
  }

  void PrintParams() {
    PrintMacro();
    std::cout << "---------PRINT PARAMETERS-------------\n"
              << "\nEvaluate dataset:, " << dataset_filename_ << std::endl
              << "kPayloadBytes:, " << payload_bytes_ << std::endl
              << "kPredictionGranularity:, " << pred_granularity_ << " records"
              << std::endl;
    if (is_on_disk_) {
      std::cout << "memory hierarchy:, on disk\n"
                << "is_compression_mode_:, " << is_compression_mode_
                << std::endl;
      if (is_compression_mode_) {
#if ALIGNED_COMPRESSION == 0
        std::cout << "\teach block is aligned to 4096" << std::endl;
#else
        std::cout << "\tthe length of each record is the same in a block, but "
                     "different in different blocks. "
                  << std::endl;
#endif
      }

      std::cout << "File size is:, " << file_bytes_ / 1024 / 1024 << " MB\n"
                << "Page size is:, " << page_bytes_ / 1024 << " KB\n"
                << "The size of each record is:, " << record_bytes_ << " bytes"
                << std::endl;
      switch (fetch_strategy_) {
        case kStartWorstCase:
          std::cout << "fetch strategy:, fetch all pages in the given search "
                       "range at once: [start, end)\n";
          break;
        case kStartOneByOne:
          std::cout << "fetch strategy:, fetch pages one by one from the "
                       "beginning of the given range: start, start + 1, ...\n";
          break;
        case kMiddleWorstCase:
          std::cout << "fetch strategy:, fetch all pages in the given search "
                       "range from the middle position: (a) mid, [mid+1, end), "
                       "or (b) "
                       "mid, [start, mid)\n";
          break;
        case kMiddleOneByOne:
          std::cout
              << "fetch strategy:, fetch pages one by one from the middle of "
                 "the given range: (a) mid, mid + 1, ... or (b) mid, mid - 1, "
                 "...\n";
          break;
      }

      std::cout << "The maximum number of records per page is:, "
                << record_num_per_page_ << " records\n"
                << "The maximum number of pages per file is:, "
                << page_num_per_file_ << " pages\n"
                << "stored position:, " << data_dir_ << std::endl;
    } else {
      std::cout << "memory hierarchy:, in memory" << std::endl;
    }
    std::cout << "---------PRINT PARAMETERS COMPLETED-------------\n";
  }
};

template <typename IndexType>
class ThreadParams {
 public:
  Params<typename IndexType::K_> params;
  IndexType index = IndexType(typename IndexType::param_t());
  typename IndexType::DataVev_ lookups;
  typename IndexType::param_t diff;  // used for testing the disk
  typename IndexType::K_ read_buf_;

  ThreadParams() {}

  ThreadParams(const Params<typename IndexType::K_>& p, const IndexType& i,
               const typename IndexType::DataVev_& l,
               const typename IndexType::param_t& d = 0)
      : params(p), index(i), lookups(l), diff(d) {}

  ThreadParams(const ThreadParams<IndexType>& other)
      : params(other.params),
        index(other.index),
        lookups(other.lookups),
        diff(other.diff) {}
};

#endif