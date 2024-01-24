#ifndef UTILS_MULTI_THREADED_BENCHMARK_H
#define UTILS_MULTI_THREADED_BENCHMARK_H

#include <chrono>

#include "../indexes/multi_threaded_hybrid/hybrid_index.h"
#include "omp.h"

template <typename IndexType>
inline void RunMultiYCSBBenchmark(DataVec& init_data, std::vector<int>& ops,
                                  KeyVec& ops_key, std::vector<int>& len,
                                  uint64_t thread_num,
                                  typename IndexType::param_t index_params) {
  std::cout << "\n\n-------- TESTING MULTI-THREADED YCSB BENCHMARK ---------"
            << std::endl;
  IndexType index(index_params);

  const uint64_t build_time = GetNsTime([&] { index.Build(init_data); }) / 1e6;
  PrintCurrentTime();

  std::cout << "\nBuild index: " << index.GetIndexName() << " over"
            << ", build_time:," << build_time << ", ms, in-memory size:,"
            << PRINT_MIB(index.GetNodeSize()) << ", MiB, total_size:,"
            << PRINT_MIB(index.GetTotalSize()) << ", MiB" << std::endl
            << std::endl;
  index.PrintEachPartSize();
  auto ops_size = ops.size();
  Value* res = new Value[thread_num];
  for (int i = 0; i < thread_num; i++) {
    res[i] = 0;
  }
  std::vector<int> insert_cnt(thread_num, 0);
  uint64_t latency_ns = 1;
  Value final_res = 0;
  auto start = std::chrono::high_resolution_clock::now();

#pragma omp parallel num_threads(thread_num)
  {
    auto thread_id = omp_get_thread_num();
#pragma omp barrier
#pragma omp master
    start = std::chrono::high_resolution_clock::now();

// running benchmark
#pragma omp for schedule(dynamic, 100)
    for (uint64_t i = 0; i < ops_size; i++) {
      switch (ops[i]) {
        case READ: {
          res[thread_id] += index.Find(ops_key[i], thread_id);
#ifdef CHECK_CORRECTION
          auto it = std::lower_bound(
              init_data.begin(), init_data.end(), ops_key[i],
              [](const auto& lhs, const Key& key) { return lhs.first < key; });
          auto tmp = index.Find(ops_key[i], thread_id);
          if (it->second != tmp || (it == init_data.end() && tmp != 0)) {
            std::cout << "tid:" << thread_id << " lookup wrong! i:" << i
                      << ",\tkey:" << ops_key[i] << std::endl;
            std::cout << "it:" << it->second << std::endl;
            std::cout << "it.idx:" << it - init_data.begin() << std::endl;
            std::cout << "tmp:" << tmp << std::endl << std::endl;
            index.Find(ops_key[i], thread_id);
          }
#endif
          break;
        }
        case UPDATE: {
          res[thread_id] +=
              index.Update(ops_key[i], Value(thread_id), thread_id);
#ifdef CHECK_CORRECTION
          auto it = std::lower_bound(
              init_data.begin(), init_data.end(), ops_key[i],
              [](const auto& lhs, const Key& key) { return lhs.first < key; });
          auto old_val = init_data[it - init_data.begin()].second;
          init_data[it - init_data.begin()].second = 0;
          auto tmp = index.Find(ops_key[i], thread_id);
          if (thread_id != tmp) {
            std::cout << "tid:" << thread_id << "Update wrong! i:" << i
                      << ",\tkey:" << ops_key[i] << std::endl;
            std::cout << "val:" << 0 << ",\told val:" << old_val << std::endl;
            std::cout << "tmp:" << tmp << ",\tit_idx:" << it - init_data.begin()
                      << std::endl;
            index.Find(ops_key[i], thread_id);
          }
#endif
          break;
        }
        case SCAN: {
          res[thread_id] += index.Scan(ops_key[i], len[i], thread_id);
          break;
        }
        case INSERT: {
          insert_cnt[thread_id]++;
          res[thread_id] +=
              index.Insert(ops_key[i], Value(thread_id), thread_id);
#ifdef CHECK_CORRECTION
          // size_t now_size = init_data.size();
          // for (int i = 0; i < thread_num; i++) {
          //   now_size += insert_cnt[i];
          // }
          // if (index.size() != now_size) {
          //   std::cout << "after insert " << i << ",\tkey:" << ops_key[i]
          //             << "some inserts are wrong!" << std::endl;
          //   index.size();
          // }
          auto tmp = index.Find(ops_key[i], thread_id);
          if (thread_id != tmp) {
            std::cout << "tid:" << thread_id << "Insert wrong! i:" << i
                      << ",\tkey:" << ops_key[i] << std::endl;
            std::cout << "val:" << thread_id << std::endl;
            std::cout << "tmp:" << tmp << std::endl;
            index.Find(ops_key[i], thread_id);
          }
#endif
          break;
        }
        default:
          break;
      }
    }  // omp for loop

#pragma omp master
    {
      const auto end = std::chrono::high_resolution_clock::now();
      latency_ns =
          std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
              .count();
      for (int i = 0; i < thread_num; i++) {
        final_res += res[i];
      }
    }
  }  // all thread join here

#ifdef BREAKDOWN
  index.PrintBreakdown();
#endif
  PrintCurrentTime();
  index.PrintEachPartSize();
  index.FreeBuffer();

  std::cout << index.GetIndexName() << ", build_time:," << build_time
            << ", ms, thread_num:," << thread_num << ", avg_time:,"
            << latency_ns * 1.0 / ops_size << ", ns,"
            << " in-memory size:," << PRINT_MIB(index.GetNodeSize())
            << ", MiB, total_size:," << PRINT_MIB(index.GetTotalSize())
            << ", MiB, #ops," << ops_size << ", throughput:,"
            << ops_size * 1.0 / latency_ns * 1e9 / 1e3 << ", K ops/s";
  std::cout << std::endl;
  std::cout << "\tfinal res:" << final_res << std::endl;
}

#endif  // !UTILS_MULTI_THREADED_BENCHMARK_H