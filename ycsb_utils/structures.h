#ifndef UTILS_STRUCTURES_H_
#define UTILS_STRUCTURES_H_

#include <unistd.h>

#include <fstream>
#include <iostream>
#include <random>

#include "../key_type.h"
#include "macro.h"

enum OpsType { READ, UPDATE, SCAN, INSERT };

enum FindStatus { kEqualToKey, kLessThanKey, kGreaterThanKey };

enum FetchStrategy { kWorstCase, kOneByOne };

struct SearchRange {
  uint64_t start;
  uint64_t stop;  // exclusive
};

struct FetchRange {
  uint64_t pid_start;
  uint64_t pid_end;
  uint64_t last_pid;
};

template <typename Value>
struct LookupInfo {
  uint64_t total_len;
  uint64_t max_len;
  Value actual_res;
};

template <typename Key, typename Value>
struct ResultInfo {
  Key res = 0;
  Value val = 0;
  int fd = 0;
  size_t pid = 0;
  size_t idx = 0;
  uint64_t fetch_page_num = 0;
  uint64_t max_search_range = 0;
  uint64_t total_search_range = 0;  // in bytes
  uint64_t total_io = 0;
  uint64_t ops = 0;
  uint64_t latency_sum = 0;
  uint64_t index_cpu_time = 0;
};

#endif