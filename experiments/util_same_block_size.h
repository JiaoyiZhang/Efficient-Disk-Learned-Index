#ifndef EXPERIMENTS_UTIL_SAME_BLOCK_SIZE_H_
#define EXPERIMENTS_UTIL_SAME_BLOCK_SIZE_H_

#include <chrono>

#include "util_search.h"

static inline uint64_t GetFileID(uint64_t idx, uint64_t recordNumPerFile) {
  return idx / recordNumPerFile;
}

static inline uint64_t GetInFilePageID(uint64_t idx, uint64_t recordNumPerPage,
                                       uint64_t pageNumPerFile) {
  return (idx / recordNumPerPage) % pageNumPerFile;
}

template <typename K>
static inline FetchRange GetFetchRange(const Params<K>& params,
                                       const SearchRange& range) {
  FetchRange fetch_range;
  fetch_range.fid_start = GetFileID(range.start, params.record_num_per_file_);
  fetch_range.pid_start = GetInFilePageID(
      range.start, params.record_num_per_page_, params.page_num_per_file_);
  fetch_range.fid_end = GetFileID(range.stop - 1, params.record_num_per_file_);
  fetch_range.pid_end = GetInFilePageID(
      range.stop - 1, params.record_num_per_page_, params.page_num_per_file_);
  return fetch_range;
}

template <typename K>
static inline std::pair<FindStatus, ResultInfo<K>> FetchPages(
    const K& lookupkey, const size_t bytes_per_page, const size_t page_num,
    const size_t record_per_page, const int fd, const size_t pid,
    const uint64_t gap_cnt, K* read_buf) {
  ResultInfo<K> res_info;
  uint64_t fetch_bytes = bytes_per_page * page_num;

#ifdef DIRECT_IO
  DirectIORead<K>(fd, bytes_per_page, page_num, pid * bytes_per_page, read_buf);
#else
  K* file_data = MMapRead<K>(filename, fetch_bytes, pid * bytes_per_page);
#endif

#ifdef PROF_CPU_IO
  auto prof_start = std::chrono::high_resolution_clock::now();
#endif  // PROF_CPU_IO
  uint64_t idx =
      LastMileSearch(read_buf, record_per_page * page_num, gap_cnt, lookupkey);
#ifdef PROF_CPU_IO
  auto prof_end = std::chrono::high_resolution_clock::now();
  uint64_t prof_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         prof_end - prof_start)
                         .count();
  last_mile_search_time += prof_ns;
#endif  // PROF_CPU_IO
  res_info.total_search_range += fetch_bytes;
  res_info.fetch_page_num += page_num;
  res_info.res = *(read_buf + idx * gap_cnt);
  res_info.total_io++;

  if (res_info.res == lookupkey) {
    return {kEqualToKey, res_info};
  } else if (res_info.res < lookupkey) {
    return {kLessThanKey, res_info};
  }
  return {kGreaterThanKey, res_info};
}

template <typename K>
static inline ResultInfo<K> WorstCaseFetch(const FetchRange range,
                                           const K lookupkey,
                                           const std::map<int, int>& open_files,
                                           const size_t bytes_per_page,
                                           const size_t record_per_page,
                                           const size_t page_num_per_file,
                                           uint64_t gap_cnt, K* read_buf) {
  bool read_page = true;
  ResultInfo<K> res_info;
  uint64_t fid = range.fid_start, pid = range.pid_start;
  while (read_page) {
    uint64_t tmp_pid_end =
        (fid == range.fid_end) ? range.pid_end : page_num_per_file - 1;
    uint64_t fetch_page_num = tmp_pid_end - pid + 1;
    int fd = open_files.find(fid)->second;
    auto fetch_res = FetchPages<K>(lookupkey, bytes_per_page, fetch_page_num,
                                   record_per_page, fd, pid, gap_cnt, read_buf);
    res_info.total_search_range += fetch_res.second.total_search_range;
    res_info.fetch_page_num += fetch_res.second.fetch_page_num;
    res_info.res = fetch_res.second.res;
    res_info.total_io += fetch_res.second.total_io;
    res_info.cpu_time += fetch_res.second.cpu_time;
    res_info.io_time += fetch_res.second.io_time;

    pid = 0;
    if (fetch_res.first == kEqualToKey || ++fid >= range.fid_end) {
      read_page = false;
    }
  }
  return res_info;
}

template <typename K>
static inline ResultInfo<K> OneByOneFetch(const FetchRange range,
                                          const K lookupkey,
                                          const std::map<int, int>& open_files,
                                          const size_t bytes_per_page,
                                          const size_t record_per_page,
                                          const size_t page_num_per_file,
                                          uint64_t gap_cnt, K* read_buf) {
  bool read_page = true;
  ResultInfo<K> res_info;
  uint64_t fid = range.fid_start, pid = range.pid_start;
  while (read_page && fid <= range.fid_end) {
    uint64_t tmp_pid_end =
        (fid == range.fid_end) ? range.pid_end : page_num_per_file - 1;
    int fd = open_files.find(fid)->second;
    while (pid <= tmp_pid_end) {
      auto fetch_res = FetchPages(lookupkey, bytes_per_page, 1, record_per_page,
                                  fd, pid, gap_cnt, read_buf);
      res_info.total_search_range += fetch_res.second.total_search_range;
      res_info.fetch_page_num += fetch_res.second.fetch_page_num;
      res_info.res = fetch_res.second.res;
      res_info.total_io += fetch_res.second.total_io;
      res_info.cpu_time += fetch_res.second.cpu_time;
      res_info.io_time += fetch_res.second.io_time;

      if (fetch_res.first == kEqualToKey) {
        read_page = false;
        break;
      }
      pid++;
    }
    fid++;
    pid = 0;
  }
  return res_info;
}

template <typename K>
static inline ResultInfo<K> OneByOneReverseFetch(
    const FetchRange range, const K lookupkey,
    const std::map<int, int>& open_files, const size_t bytes_per_page,
    const size_t record_per_page, const size_t page_num_per_file,
    uint64_t gap_cnt, K* read_buf) {
  bool read_page = true;
  ResultInfo<K> res_info;
  uint64_t fid = range.fid_end, pid = range.pid_end;
  while (read_page && fid >= range.fid_start) {
    uint64_t tmp_pid_start = (fid == range.fid_start) ? range.pid_start : 0;
    int fd = open_files.find(fid)->second;
    while (pid >= tmp_pid_start) {
      auto fetch_res = FetchPages(lookupkey, bytes_per_page, 1, record_per_page,
                                  fd, pid, gap_cnt, read_buf);
      res_info.total_search_range += fetch_res.second.total_search_range;
      res_info.fetch_page_num += fetch_res.second.fetch_page_num;
      res_info.res = fetch_res.second.res;
      res_info.total_io += fetch_res.second.total_io;
      res_info.cpu_time += fetch_res.second.cpu_time;
      res_info.io_time += fetch_res.second.io_time;

      if (fetch_res.first == kEqualToKey) {
        read_page = false;
        break;
      }
      pid--;
    }
    fid--;
    pid = page_num_per_file - 1;
  }
  return res_info;
}

template <typename K>
static inline ResultInfo<K> NormalCoreLookup(const SearchRange& range,
                                             const K& lookupkey,
                                             const Params<K>& params,
                                             uint64_t gap_cnt) {
  ResultInfo<K> res_info;
  FetchRange fetch_range;
  fetch_range = GetFetchRange<K>(params, range);

  switch (params.fetch_strategy_) {
    case kStartWorstCase: {
      res_info = WorstCaseFetch<K>(
          fetch_range, lookupkey, params.open_files, params.page_bytes_,
          params.record_num_per_page_, params.page_num_per_file_, gap_cnt,
          params.read_buf_);
      break;
    }
    case kStartOneByOne: {
      res_info = OneByOneFetch<K>(
          fetch_range, lookupkey, params.open_files, params.page_bytes_,
          params.record_num_per_page_, params.page_num_per_file_, gap_cnt,
          params.read_buf_);
      break;
    }
    case kMiddleWorstCase: {
      uint64_t mid = (range.start + range.stop) >> 1;
      uint64_t mid_fid = GetFileID(mid, params.record_num_per_file_);
      uint64_t mid_pid = GetInFilePageID(mid, params.record_num_per_page_,
                                         params.page_num_per_file_);
      int fd = params.open_files.find(mid_fid)->second;
      auto fetch_res = FetchPages(lookupkey, params.page_bytes_, 1,
                                  params.record_num_per_page_, fd, mid_pid,
                                  gap_cnt, params.read_buf_);
      res_info = fetch_res.second;

#ifdef PROF_CPU_IO
      const auto prof_start = std::chrono::high_resolution_clock::now();
#endif  // PROF_CPU_IO

      if (fetch_res.first == kEqualToKey) {
        break;

      } else if (fetch_res.first == kLessThanKey) {
        // [mid + 1, end)
        if (mid_pid + 1 < params.page_num_per_file_) {
          fetch_range.pid_start = mid_pid + 1;
          fetch_range.fid_start = mid_fid;
        } else if (mid_fid + 1 <= fetch_range.fid_end) {
          fetch_range.pid_start = 0;
          fetch_range.fid_start = mid_fid + 1;
        } else {
          break;
        }

      } else {
        // [start, mid)
        if (mid_pid >= 1) {
          fetch_range.pid_end = mid_pid - 1;
          fetch_range.fid_end = mid_fid;
        } else if (mid_fid >= 1) {
          fetch_range.fid_end = mid_fid - 1;
          fetch_range.pid_end = params.page_num_per_file_ - 1;
        } else {
          break;
        }
      }

#ifdef PROF_CPU_IO
      const auto prof_end = std::chrono::high_resolution_clock::now();
      uint64_t prof_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             prof_end - prof_start)
                             .count();
      res_info.cpu_time += prof_ns;
#endif  // PROF_CPU_IO

      auto second_res = WorstCaseFetch<K>(
          fetch_range, lookupkey, params.open_files, params.page_bytes_,
          params.record_num_per_page_, params.page_num_per_file_, gap_cnt,
          params.read_buf_);

      res_info.total_search_range += second_res.total_search_range;
      res_info.fetch_page_num += second_res.fetch_page_num;
      res_info.res = second_res.res;
      res_info.total_io += second_res.total_io;
      res_info.cpu_time += second_res.cpu_time;
      res_info.io_time += second_res.io_time;

      break;
    }
    case kMiddleOneByOne: {
      uint64_t mid = (range.start + range.stop) >> 1;
      uint64_t mid_fid = GetFileID(mid, params.record_num_per_file_);
      uint64_t mid_pid = GetInFilePageID(mid, params.record_num_per_page_,
                                         params.page_num_per_file_);
      int fd = params.open_files.find(mid_fid)->second;
      auto fetch_res = FetchPages(lookupkey, params.page_bytes_, 1,
                                  params.record_num_per_page_, fd, mid_pid,
                                  gap_cnt, params.read_buf_);
      res_info = fetch_res.second;

      if (fetch_res.first == kEqualToKey) {
        break;

      } else if (fetch_res.first == kLessThanKey) {
#ifdef PROF_CPU_IO
        const auto prof_start = std::chrono::high_resolution_clock::now();
#endif  // PROF_CPU_IO

        // mid + 1, mid + 2, ..., end - 1
        if (mid_pid + 1 < params.page_num_per_file_) {
          fetch_range.pid_start = mid_pid + 1;
          fetch_range.fid_start = mid_fid;
        } else if (mid_fid + 1 <= fetch_range.fid_end) {
          fetch_range.pid_start = 0;
          fetch_range.fid_start = mid_fid + 1;
        } else {
          break;
        }

#ifdef PROF_CPU_IO
        const auto prof_end = std::chrono::high_resolution_clock::now();
        uint64_t prof_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               prof_end - prof_start)
                               .count();
        res_info.cpu_time += prof_ns;
#endif  // PROF_CPU_IO

        auto second_res = OneByOneFetch<K>(
            fetch_range, lookupkey, params.open_files, params.page_bytes_,
            params.record_num_per_page_, params.page_num_per_file_, gap_cnt,
            params.read_buf_);

        res_info.total_search_range += second_res.total_search_range;
        res_info.fetch_page_num += second_res.fetch_page_num;
        res_info.res = second_res.res;
        res_info.total_io += second_res.total_io;
        res_info.cpu_time += second_res.cpu_time;
        res_info.io_time += second_res.io_time;

      } else {
#ifdef PROF_CPU_IO
        const auto prof_start = std::chrono::high_resolution_clock::now();
#endif  // PROF_CPU_IO

        // mid - 1, mid - 2, ..., start
        if (mid_pid >= 1) {
          fetch_range.pid_end = mid_pid - 1;
          fetch_range.fid_end = mid_fid;
        } else if (mid_fid >= 1) {
          fetch_range.fid_end = mid_fid - 1;
          fetch_range.pid_end = params.page_num_per_file_ - 1;
        } else {
          break;
        }

#ifdef PROF_CPU_IO
        const auto prof_end = std::chrono::high_resolution_clock::now();
        uint64_t prof_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               prof_end - prof_start)
                               .count();
        res_info.cpu_time += prof_ns;
#endif  // PROF_CPU_IO

        auto second_res = OneByOneReverseFetch<K>(
            fetch_range, lookupkey, params.open_files, params.page_bytes_,
            params.record_num_per_page_, params.page_num_per_file_, gap_cnt,
            params.read_buf_);

        res_info.total_search_range += second_res.total_search_range;
        res_info.fetch_page_num += second_res.fetch_page_num;
        res_info.res = second_res.res;
        res_info.total_io += second_res.total_io;
        res_info.cpu_time += second_res.cpu_time;
        res_info.io_time += second_res.io_time;
      }

      break;
    }
    case kLecoFetch: {
      // [start, end / 2)
      uint64_t mid = (range.start + range.stop) >> 1;
      uint64_t mid_fid = GetFileID(mid, params.record_num_per_file_);
      uint64_t mid_pid = GetInFilePageID(mid, params.record_num_per_page_,
                                         params.page_num_per_file_);
      auto half_range = fetch_range;
      half_range.pid_end = mid_pid - 1;
      half_range.fid_end = mid_fid;
      res_info = WorstCaseFetch<K>(
          half_range, lookupkey, params.open_files, params.page_bytes_,
          params.record_num_per_page_, params.page_num_per_file_, gap_cnt,
          params.read_buf_);

      if (res_info.res != lookupkey) {
#ifdef PROF_CPU_IO
        const auto prof_start = std::chrono::high_resolution_clock::now();
#endif  // PROF_CPU_IO

        // search within the remaining range: [mid, end)
        half_range.pid_end = fetch_range.pid_end;
        half_range.fid_end = fetch_range.fid_end;
        if (mid_pid < params.page_num_per_file_) {
          half_range.pid_start = mid_pid;
          half_range.fid_start = mid_fid;
        } else if (mid_fid + 1 <= fetch_range.fid_end) {
          half_range.pid_start = 0;
          half_range.fid_start = mid_fid + 1;
        } else {
          break;
        }

#ifdef PROF_CPU_IO
        const auto prof_end = std::chrono::high_resolution_clock::now();
        uint64_t prof_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               prof_end - prof_start)
                               .count();
        res_info.cpu_time += prof_ns;
#endif  // PROF_CPU_IO

        auto second_res = WorstCaseFetch<K>(
            half_range, lookupkey, params.open_files, params.page_bytes_,
            params.record_num_per_page_, params.page_num_per_file_, gap_cnt,
            params.read_buf_);
        res_info.total_search_range += second_res.total_search_range;
        res_info.fetch_page_num += second_res.fetch_page_num;
        res_info.res = second_res.res;
        res_info.total_io += second_res.total_io;
        res_info.cpu_time += second_res.cpu_time;
        res_info.io_time += second_res.io_time;
      }

      break;
    }
  }

  return res_info;
}

#endif  // EXPERIMENTS_UTIL_SAME_BLOCK_SIZE_H_