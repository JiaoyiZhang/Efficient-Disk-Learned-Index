#ifndef EXPERIMENTS_UTIL_LID_H_
#define EXPERIMENTS_UTIL_LID_H_

#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <random>
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

static inline void PrintBaseInfo() {
  std::cout << "\n=== CPU info ===" << std::endl;
  std::cout << "core number, processor" << std::endl;
  system("cat /proc/cpuinfo | grep name | cut -f2 -d: | uniq -c");
  system("lscpu -C");
  std::cout << "\n=== Mem info ===" << std::endl;
  system("free -m");
  std::cout << "\n=== Device info ===" << std::endl;
  system("lsblk -d -o name,rota");
  // std::cout << "\n=== IO info ===" << std::endl;
  // system("cat /proc/$PPID/io");
}

template <typename K>
static std::vector<K> LoadKeys(const std::string& file, bool read_res = false,
                               uint64_t* actual_res = NULL) {
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
    if (read_res) {
      data_file.read(reinterpret_cast<char*>(actual_res), sizeof(uint64_t));
    }
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

int DirectIOOpen(const std::string& filename) {
#ifdef __APPLE__
  // Reference:
  // https://github.com/facebook/rocksdb/wiki/Direct-IO
  int fd = open(filename.c_str(), O_RDONLY);
  fcntl(fd, F_NOCACHE, 1);
#else  // Linux
  int fd = open(filename.c_str(), O_RDONLY | O_DIRECT | O_SYNC);
#endif
  if (fd == -1) {
    throw std::runtime_error("open file error in DirectIOOpen");
  }
  return fd;
}

void DirectIOClose(int fd) { close(fd); }

std::map<int, int> OpenFiles(const std::string& dir, int file_num) {
  std::map<int, int> files;
  for (int idx = 0; idx < file_num; idx++) {
    std::string file = dir + std::to_string(idx) + ".data";
    int fd = DirectIOOpen(file);
    files.insert({idx, fd});
  }
  return files;
}

template <typename K>
static void StoreData(std::vector<K> keys, Params<K>& params) {
  int file_num = std::ceil(keys.size() * 1.0 / params.record_num_per_file_);
  std::vector<char> payload(params.payload_bytes_, 'a');
  uint64_t s = 0, e = keys.size();
  std::string file;
  for (int idx = 0; idx < file_num; idx++) {
    file = params.data_dir_ + std::to_string(idx) + ".data";
    s = idx * params.record_num_per_file_;
    e = std::min<size_t>((idx + 1) * params.record_num_per_file_, keys.size());
    char* data = MapFile<char>(file, params.record_bytes_ * (e - s));
    for (uint64_t i = s; i < e; i++) {
      memcpy(data, &keys[i], sizeof(K));
      data += sizeof(K);
      memcpy(data, &payload, params.payload_bytes_);
      data += params.payload_bytes_;
    }
  }
  if (keys.size() / params.record_num_per_page_ != 0) {
    uint64_t num = params.record_num_per_page_ -
                   (keys.size() % params.record_num_per_page_);
    char* data = MapFile<char>(file, (e + num) * params.record_bytes_);
    data += params.record_bytes_ * (e - s);
    K max_key = keys.back();
    for (uint64_t i = 0; i < num; i++) {
      memcpy(data, &max_key, sizeof(K));
      data += sizeof(K);
      memcpy(data, &payload, params.payload_bytes_);
      data += params.payload_bytes_;
    }
  }
  std::cout << "store " << keys.size() << " records into " << params.data_dir_;
}

template <typename K, typename V>
static LookupInfo<V> GetLookupInfo(const std::vector<std::pair<K, V>>& data,
                                   const std::vector<std::pair<K, V>>& lookups,
                                   bool is_on_disk) {
  LookupInfo<V> lookup_info = {0, 0, 0};
  // Get the sum over all values with each lookup key
  for (auto record : lookups) {
    auto it = std::lower_bound(
        data.begin(), data.end(), record.first,
        [](const auto& lhs, const K key) { return lhs.first < key; });
    lookup_info.actual_res += it->first;
    uint64_t tmp_len = 1;
    if (!is_on_disk) {
      while (++it != data.end() && it->first == record.first &&
             tmp_len < MAX_NUM_QUALIFYING) {
        lookup_info.actual_res += it->first;
        tmp_len++;
      }
    }
    lookup_info.total_len += tmp_len;
    if (tmp_len > lookup_info.max_len) {
      lookup_info.max_len = tmp_len;
    }
  }
  return lookup_info;
}

template <typename K, typename V>
static std::vector<std::pair<K, V>> GetLookupKeys(
    const std::string& file, const std::vector<std::pair<K, V>>& data,
    uint64_t num, bool first_run, bool is_on_disk, LookupInfo<V>* lookup_info) {
  PrintCurrentTime();
  std::vector<std::pair<K, V>> keys(num);
  uint64_t ms;
  if (first_run) {
    std::random_device rd;
    std::uniform_int_distribution<int> dis(0, data.size() - 1);

    ms = GetNsTime([&] {
           for (uint64_t i = 0; i < num; i++) {
             auto idx = dis(rd);
             keys[i] = data[idx];
           }
         }) /
         1e6;

    *lookup_info = GetLookupInfo<K, V>(data, keys, is_on_disk);

    char* storedKeys = MapFile<char>(
        file, sizeof(std::pair<K, V>) * keys.size() + sizeof(uint64_t) * 2);
    memcpy(storedKeys, &num, sizeof(uint64_t));
    storedKeys += sizeof(uint64_t);
    memcpy(storedKeys, &(lookup_info->actual_res), sizeof(uint64_t));
    storedKeys += sizeof(uint64_t);
    for (uint64_t i = 0; i < keys.size(); i++) {
      memcpy(storedKeys, &keys[i], sizeof(std::pair<K, V>));
      storedKeys += sizeof(std::pair<K, V>);
    }
    std::cout << "get " << keys.size() << " lookup keys from " << data.size()
              << " records in " << ms << " ms ("
              << static_cast<double>(data.size()) / 1000 / ms << " M values/s)"
              << std::endl;
  } else {
    keys = LoadKeys<std::pair<K, V>>(file, true, &(lookup_info->actual_res));
  }

  return keys;
}

#if ALIGNED_COMPRESSION == 0
template <typename K>
static std::vector<std::pair<K, uint64_t>> StoreCompressedData(
    const std::vector<K>& keys, const Params<K>& params) {
  std::vector<std::pair<K, uint64_t>> compressed_data, stored_data;
  std::vector<K> stored_key;

  // first need to record the byte_offset of each key
  auto key_it = params.comp_block_bytes.key_num_.begin();
  auto byte_it = params.comp_block_bytes.byte_offset_.begin();
  uint64_t record_per_page = getpagesize() / params.record_bytes_;
  size_t offset = 0;
  uint64_t last_page_num = 0;
  for (uint64_t i = 0; i < keys.size(); i++) {
    compressed_data.push_back({keys[i], offset});
    stored_key.push_back(keys[i]);
    if ((i + 1) == *key_it) {
      auto stored_num =
          record_per_page - ((*key_it) - last_page_num) % record_per_page;
      if ((*key_it) - last_page_num == record_per_page) {
        stored_num = 0;
      }
      last_page_num = *key_it;
      key_it++;
      for (uint64_t j = 0; j < stored_num; j++) {
        stored_key.push_back(UINT64_MAX);
      }
      offset = *byte_it;
      byte_it++;
    } else {
      offset += params.record_bytes_;
    }
  }

  StoreData<K>(stored_key, params);
  return compressed_data;
}
#else
template <typename K>
static std::vector<std::pair<K, uint64_t>> StoreCompressedData(
    const std::vector<K>& keys, const Params<K>& params) {
  std::vector<std::pair<K, uint64_t>> compressed_data;
  char* data = MapFile<char>(params.data_dir_ + "0.data", params.file_bytes_);
  auto record_bytes_it = params.comp_block_bytes.key_num_.begin();
  size_t offset = 0;
  auto payload_bytes =
      (*record_bytes_it) > 0 ? (*record_bytes_it) - sizeof(K) : 0;
  std::vector<char> payload(payload_bytes, 'a');
  for (uint64_t i = 0; i < keys.size(); i++) {
    compressed_data.push_back({keys[i], offset});
    memcpy(data, &keys[i], sizeof(K));
    data += sizeof(K);
    memcpy(data, &payload, payload_bytes);
    data += payload_bytes;
    offset += *record_bytes_it;
    if ((i + 1) % params.record_num_per_page_ == 0) {
      record_bytes_it++;
      payload_bytes =
          (*record_bytes_it) > 0 ? (*record_bytes_it) - sizeof(K) : 0;
      payload = std::vector<char>(payload_bytes, 'a');
    }
  }

  std::cout << "store " << keys.size() << " records into " << params.data_dir_
            << std::endl;

  return compressed_data;
}
#endif

#endif  // EXPERIMENTS_UTIL_LID_H_