#ifndef EXPERIMENTS_UTIL_SEARCH_H_
#define EXPERIMENTS_UTIL_SEARCH_H_

#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <iostream>

#include "./structures.h"

int DirectIOOpen(const std::string& filename) {
#ifdef __APPLE__
  // Reference:
  // https://github.com/facebook/rocksdb/wiki/Direct-IO
  int fd = open(filename.c_str(), O_RDONLY);
  fcntl(fd, F_NOCACHE, 1);
#else  // Linux
  int fd = open(filename.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0644);
#ifdef PRINT_PROCESSING_INFO
  std::cout << "DirectIOOpen file:" << filename << ",\tfd:" << fd << std::endl;
#endif
#endif
  if (fd == -1) {
    throw std::runtime_error("open file error in DirectIOOpen");
  }
  return fd;
}

void DirectIOClose(int fd) {
#ifdef PRINT_PROCESSING_INFO
  std::cout << "DirectIOClose file:" << fd << std::endl;
#endif
  close(fd);
}

template <typename K>
static void DirectIORead(int fd, size_t page_bytes, size_t page_num,
                         size_t offset, K* read_buf) {
  int ret = pread(fd, read_buf, page_bytes * page_num, offset);
  if (ret == -1) {
    throw std::runtime_error("read error in DirectIORead");
  }
}

template <typename ElementType>
static void DirectIOWrite(int fd, const std::vector<ElementType>& data,
                          size_t page_bytes, size_t page_num, void* write_buf,
                          size_t seek_offset = 0) {
  int total_num = page_num;
  while (total_num > 0) {
    int tmp_num = total_num;
    if (tmp_num > 500000) {
      tmp_num = 500000;
    }
    size_t offset = page_bytes * (page_num - total_num);
    size_t cpy_size = std::min(tmp_num * page_bytes,
                               data.size() * sizeof(ElementType) - offset);
    memcpy(write_buf, &data[offset / sizeof(ElementType)], cpy_size);
    auto ret =
        pwrite(fd, write_buf, page_bytes * tmp_num, seek_offset + offset);

    if (ret == -1) {
      throw std::runtime_error("write error in DirectIOWrite");
    }
    total_num -= tmp_num;
  }
}

template <typename K, typename V>
static bool Update1Page(int fd, size_t pid, size_t idx, K key, V value,
                        size_t page_bytes, K* buf) {
  DirectIORead<K>(fd, page_bytes, 1, page_bytes * pid, buf);
  int gap_cnt = (sizeof(V) + sizeof(K)) / sizeof(K);
  K find_k = *(buf + idx * gap_cnt);
  if (find_k == key) {
    *(buf + idx * gap_cnt + 1) = value;
    size_t offset = page_bytes * pid;
    lseek(fd, offset, SEEK_SET);
    int ret = write(fd, buf, page_bytes);
    if (ret == -1) {
      throw std::runtime_error("write error in Update1Page");
    }
  } else {
    return false;
  }
  return true;
}

template <typename K>
inline uint64_t LastMileSearch(const K* data, uint64_t record_num,
                               uint64_t gap_cnt, K key) {
  uint64_t s = 0, e = record_num;
  if (*(data + (e - 1) * gap_cnt) < key) {
    return e - 1;
  }

  while (s < e) {
    uint64_t mid = (s + e) >> 1;
    if (*(data + mid * gap_cnt) < key)
      s = mid + 1;
    else
      e = mid;
  }
  return s;
}

template <typename K, typename V>
static inline void GetAllData(int fd, const size_t start_page_id,
                              const size_t page_num,
                              const size_t record_per_page,
                              const uint64_t length, K* read_buf,
                              std::vector<std::pair<K, V>>& data) {
  uint64_t bytes_per_page = record_per_page * (sizeof(V) + sizeof(K));
  uint64_t gap_cnt = (sizeof(V) + sizeof(K)) / sizeof(K);
  size_t idx = 0, item_offset = 0;
  int total_num = page_num;
  while (total_num > 0) {
    int tmp_num = total_num;
    if (tmp_num > 500000) {
      tmp_num = 500000;
    }
    DirectIORead<K>(fd, bytes_per_page, tmp_num,
                    bytes_per_page * (start_page_id + page_num - total_num),
                    read_buf);
    size_t copy_size =
        (sizeof(K) + sizeof(V)) *
        std::min(length - item_offset, tmp_num * record_per_page);
    memcpy(&data[item_offset], read_buf, copy_size);
    total_num -= tmp_num;
    item_offset += tmp_num * record_per_page;
  }
}

template <typename K, typename V>
static inline ResultInfo<K, V> RangeScan(int fd, const size_t pid,
                                         const size_t page_num,
                                         const size_t record_per_page,
                                         const uint64_t length, K* read_buf) {
  ResultInfo<K, V> res_info;
  uint64_t bytes_per_page = record_per_page * sizeof(Record);
  uint64_t fetch_bytes = bytes_per_page * page_num;
  uint64_t gap_cnt = (sizeof(V) + sizeof(K)) / sizeof(K);

  DirectIORead<K>(fd, bytes_per_page, page_num, pid * bytes_per_page, read_buf);
  res_info.total_search_range += fetch_bytes;
  res_info.fetch_page_num += page_num;
  res_info.total_io++;

  for (size_t idx = 0; idx < length; idx++) {
    res_info.res += *(read_buf + idx * gap_cnt);
    res_info.val += *(read_buf + idx * gap_cnt + 1);
  }
  return res_info;
}

template <typename K, typename V>
static inline std::pair<FindStatus, ResultInfo<K, V>> FetchPages(
    int fd, const K& lookupkey, const size_t page_num,
    const size_t record_per_page, const size_t pid, const size_t last_pid,
    const uint64_t length, K* read_buf, int last_id) {
  ResultInfo<K, V> res_info;
  uint64_t bytes_per_page = record_per_page * sizeof(Record);
  uint64_t fetch_bytes = bytes_per_page * page_num;
  uint64_t gap_cnt = (sizeof(V) + sizeof(K)) / sizeof(K);

  DirectIORead<K>(fd, bytes_per_page, page_num, pid * bytes_per_page, read_buf);

  uint64_t idx = LastMileSearch(
      read_buf, record_per_page * (page_num - 1) + last_id, gap_cnt, lookupkey);
  res_info.total_search_range += fetch_bytes;
  res_info.fetch_page_num += page_num;
  res_info.res = *(read_buf + idx * gap_cnt);
  res_info.val = *(read_buf + idx * gap_cnt + 1);
  res_info.total_io++;

  if (res_info.res == lookupkey) {
    res_info.fd = fd;
    res_info.pid = (idx / record_per_page) + pid;
    res_info.idx = idx % record_per_page;
    // for range scan
    auto len = length - 1;
    while (len && (++idx) < record_per_page * page_num) {
      res_info.res += *(read_buf + idx * gap_cnt);
      res_info.val += *(read_buf + idx * gap_cnt + 1);
      len--;
    }
    if (len && pid <= last_pid) {
      size_t remain_page = std::ceil(len / record_per_page);
      remain_page = std::min(remain_page, last_pid - pid);
      auto remain_res = RangeScan<K, V>(fd, pid + 1, remain_page,
                                        record_per_page, len, read_buf);
      res_info.total_search_range += remain_res.total_search_range;
      res_info.fetch_page_num += remain_res.fetch_page_num;
      res_info.res += remain_res.res;
      res_info.val += remain_res.val;
      res_info.total_io += remain_res.total_io;
    }
    return {kEqualToKey, res_info};
  } else if (res_info.res < lookupkey) {
    return {kLessThanKey, res_info};
  }
  return {kGreaterThanKey, res_info};
}

template <typename K, typename V>
static inline ResultInfo<K, V> WorstCaseFetch(const FetchRange range,
                                              const K lookupkey, int fd,
                                              const size_t record_per_page,
                                              const uint64_t length,
                                              K* read_buf, int last_id) {
  ResultInfo<K, V> res_info;
  uint64_t fetch_page_num = range.pid_end - range.pid_start + 1;
  auto fetch_res = FetchPages<K, V>(fd, lookupkey, fetch_page_num,
                                    record_per_page, range.pid_start,
                                    range.last_pid, length, read_buf, last_id);

  res_info = fetch_res.second;
  return res_info;
}

template <typename K, typename V>
static inline ResultInfo<K, V> OneByOneFetch(const FetchRange range,
                                             const K lookupkey, int fd,
                                             const size_t record_per_page,
                                             const uint64_t length, K* read_buf,
                                             int last_id) {
  ResultInfo<K, V> res_info;
  uint64_t pid = range.pid_start;
  while (pid <= range.pid_end) {
    auto fetch_res =
        FetchPages<K, V>(fd, lookupkey, 1, record_per_page, pid, range.last_pid,
                         length, read_buf, last_id);
    res_info.total_search_range += fetch_res.second.total_search_range;
    res_info.fetch_page_num += fetch_res.second.fetch_page_num;
    res_info.res = fetch_res.second.res;
    res_info.val = fetch_res.second.val;
    res_info.total_io += fetch_res.second.total_io;
    res_info.fd = fetch_res.second.fd;
    res_info.pid = fetch_res.second.pid;
    res_info.idx = fetch_res.second.idx;

    if (fetch_res.first == kEqualToKey) {
      break;
    }
    pid++;
  }
  return res_info;
}

static inline FetchRange GetFetchRange(const SearchRange& range,
                                       uint64_t record_per_page,
                                       uint64_t last_pid) {
  FetchRange fetch_range;
  fetch_range.last_pid = last_pid;
  fetch_range.pid_start = range.start / record_per_page;
  fetch_range.pid_end = std::min((range.stop - 1) / record_per_page, last_pid);
  return fetch_range;
}

template <typename K, typename V>
static inline ResultInfo<K, V> NormalCoreLookup(
    int fd, const SearchRange& range, const K& lookupkey,
    const FetchStrategy& fetch_strategy, uint64_t record_per_page,
    uint64_t length, uint64_t last_pid, K* read_buf, int last_id,
    size_t pid_offset = 0) {
  ResultInfo<K, V> res_info;
  FetchRange fetch_range = GetFetchRange(range, record_per_page, last_pid);
  fetch_range.pid_start += pid_offset;
  fetch_range.pid_end += pid_offset;
  switch (fetch_strategy) {
    case kWorstCase: {
      res_info =
          WorstCaseFetch<K, V>(fetch_range, lookupkey, fd, record_per_page,
                               length, read_buf, last_id);
      break;
    }
    case kOneByOne: {
      res_info =
          OneByOneFetch<K, V>(fetch_range, lookupkey, fd, record_per_page,
                              length, read_buf, last_id);
      break;
    }
  }

  return res_info;
}

#endif