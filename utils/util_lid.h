#ifndef UTILS_UTIL_LID_H_
#define UTILS_UTIL_LID_H_

#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "structures.h"

static inline uint64_t GetNsTime(std::function<void()> fn) {
  const auto start = std::chrono::high_resolution_clock::now();
  fn();
  const auto end = std::chrono::high_resolution_clock::now();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
      .count();
}

static inline void PrintCurrentTime() {
  time_t timep;
  time(&timep);
  char tmpTime[64];
  strftime(tmpTime, sizeof(tmpTime), "%Y-%m-%d %H:%M:%S", localtime(&timep));
  std::cout << "TEST time: " << tmpTime << std::endl;
}

template <typename K>
inline std::vector<K> LoadKeys(const std::string& file) {
  // Consistent with SOSD
  std::vector<K> data;
  const uint64_t ns = GetNsTime([&] {
    std::ifstream data_file(file, std::ios::binary);
    if (!data_file.is_open()) {
      std::cerr << "unable to open " << file << std::endl;
      exit(EXIT_FAILURE);
    }
    // Read size.
    uint64_t size;
    data_file.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
    data.resize(size);

    // Read values.
    data_file.read(reinterpret_cast<char*>(data.data()), size * sizeof(K));
    data_file.close();
  });

  const uint64_t ms = ns / 1e6;

  std::cout << "\nread " << data.size() << " values from " << file << " in "
            << ms << " ms (" << static_cast<double>(data.size()) / 1000 / ms
            << " M values/s)" << std::endl;

  return data;
}

template <typename K>
static K* MapFile(const std::string& filename, size_t page_bytes) {
  int fd = open(filename.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd == -1) {
    std::cout << "MapFile open:" << filename << std::endl;
    throw std::runtime_error("Open file error in util.h/MapFile");
  }
  ftruncate(fd, page_bytes);

  auto data = reinterpret_cast<K*>(
      mmap(nullptr, page_bytes, PROT_WRITE, MAP_SHARED, fd, 0));
  if (data == MAP_FAILED) {
    struct stat fs;
    stat(filename.c_str(), &fs);
    std::cout << "MapFile mmap: filename" << filename
              << ",\t page_bytes: " << page_bytes
              << ",\t file size: " << fs.st_size << std::endl;
    throw std::runtime_error("mmap error in MapFile");
  }
  close(fd);
  return data;
}

template <typename K>
static void UnmapFile(K* data, size_t file_bytes) {
  if (data && munmap(data, file_bytes)) {
    throw std::runtime_error("munmap error");
  }
}

template <typename K>
static void StoreData(std::vector<K> keys, const std::string& filepath) {
  char* data =
      MapFile<char>(filepath, sizeof(K) * keys.size() + sizeof(uint64_t));
  auto size = keys.size();
  memcpy(data, &size, sizeof(uint64_t));
  memcpy(data + sizeof(uint64_t), &keys[0], sizeof(K) * keys.size());

  std::cout << "store " << keys.size() << " records into " << filepath
            << std::endl;
}

#endif  // UTILS_UTIL_LID_H_