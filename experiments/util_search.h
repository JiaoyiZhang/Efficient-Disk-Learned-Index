#ifndef EXPERIMENTS_UTIL_SEARCH_H_
#define EXPERIMENTS_UTIL_SEARCH_H_

#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>
#include <iostream>

#include "structures.h"

template <typename K>
static K* MMapRead(const std::string& filename, size_t page_bytes,
                   size_t offset) {
  int fd = open(filename.c_str(), O_RDONLY);
  if (fd == -1) {
    std::cout << "MapFile open:" << filename << std::endl;
    throw std::runtime_error("Open file error in MMapRead");
  }

  auto data = reinterpret_cast<K*>(
      mmap(nullptr, page_bytes, PROT_READ, MAP_SHARED, fd, offset));
  if (data == MAP_FAILED) {
    struct stat fs;
    stat(filename.c_str(), &fs);
    std::cout << "MMapRead mmap: filename" << filename
              << ",\t page_bytes: " << page_bytes << ",\t offset:" << offset
              << ",\t file size: " << fs.st_size << std::endl;
    throw std::runtime_error("mmap error in MapFile");
  }
  close(fd);
  return data;
}

uint64_t prof_io_time = 0;
uint64_t last_mile_search_time = 0;
uint64_t prof_file_cpu_time = 0;

template <typename K>
static void DirectIORead(int fd, size_t page_bytes, size_t page_num,
                         size_t offset, K* read_buf) {
#ifdef PROF_CPU_IO
  auto prof_start = std::chrono::high_resolution_clock::now();
#endif  // PROF_CPU_IO
  if (lseek(fd, offset, SEEK_SET) == -1) {
    throw std::runtime_error("lseek file error in DirectIORead");
  }
#ifdef PROF_CPU_IO
  auto prof_end = std::chrono::high_resolution_clock::now();
  uint64_t prof_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         prof_end - prof_start)
                         .count();
  prof_file_cpu_time += prof_ns;

  prof_start = std::chrono::high_resolution_clock::now();
#endif  // PROF_CPU_IO

  int ret = read(fd, read_buf, page_bytes * page_num);
  if (ret == -1) {
    throw std::runtime_error("read error in DirectIORead");
  }

#ifdef PROF_CPU_IO
  prof_end = std::chrono::high_resolution_clock::now();
  prof_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(prof_end -
                                                                 prof_start)
                .count();
  prof_io_time += prof_ns;
#endif  // PROF_CPU_IO
}

template <typename K>
inline uint64_t LastMileSearch(const K* data, uint64_t record_num,
                               uint64_t gap_cnt, K key) {
  // Here assuming that each location has a meaningful value
  uint64_t s = 0, e = record_num;
  if (*(data + (e - 1) * gap_cnt) < key) {
    return e - 1;
  }

#if LAST_MILE_SEARCH == 0
  // binary search
  while (s < e) {
    uint64_t mid = (s + e) >> 1;
    if (*(data + mid * gap_cnt) < key)
      s = mid + 1;
    else
      e = mid;
  }
  return s;
#else
  // linear search
  while (s < e) {
    if (*(data + s * gap_cnt) == key)
      return s;
    else
      s++;
  }
  return s;
#endif
}

#endif