#ifndef UTILS_BENCHMARK_H
#define UTILS_BENCHMARK_H

#include "../indexes/hybrid/hybrid_index.h"

template <typename IndexType>
inline void RunYCSBBenchmark(DataVec& init_data, std::vector<int>& ops,
                             KeyVec& ops_key, std::vector<int>& len,
                             typename IndexType::param_t index_params) {
  std::cout << "\n\n--------------- TESTING YCSB BENCHMARK ----------------"
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
  Value res = 0;
  auto ops_size = ops.size();
  uint64_t ns = GetNsTime([&] {
    for (uint64_t i = 0; i < ops_size; i++) {
      switch (ops[i]) {
        case READ: {
          res += index.Find(ops_key[i]);
#ifdef CHECK_CORRECTION
          auto tmp = index.Find(ops_key[i]);
          auto it = std::lower_bound(
              init_data.begin(), init_data.end(), ops_key[i],
              [](const auto& lhs, const Key& key) { return lhs.first < key; });
          if (it->second != tmp || (it == init_data.end() && tmp != 0)) {
            std::cout << "lookup wrong! i:" << i << ",\tkey:" << ops_key[i]
                      << std::endl;
            std::cout << "it:" << it->second << std::endl;
            std::cout << "it.idx:" << it - init_data.begin() << std::endl;
            std::cout << "tmp:" << tmp << std::endl;
            index.Find(ops_key[i]);
          }
#endif
          break;
        }
        case UPDATE: {
          res += index.Update(ops_key[i], Value(0));
#ifdef CHECK_CORRECTION
          auto it = std::lower_bound(
              init_data.begin(), init_data.end(), ops_key[i],
              [](const auto& lhs, const Key& key) { return lhs.first < key; });
          auto old_val = init_data[it - init_data.begin()].second;
          init_data[it - init_data.begin()].second = 0;
          auto tmp = index.Find(ops_key[i]);
          if (0 != tmp) {
            std::cout << "Update wrong! i:" << i << ",\tkey:" << ops_key[i]
                      << std::endl;
            std::cout << "val:" << 0 << ",\told val:" << old_val << std::endl;
            std::cout << "tmp:" << tmp << ",\tit_idx:" << it - init_data.begin()
                      << std::endl;
            index.Find(ops_key[i]);
          }
#endif
          break;
        }
        case SCAN: {
          res += index.Scan(ops_key[i], len[i]);
          break;
        }
        case INSERT: {
          res += index.Insert(ops_key[i], Value(0));
#ifdef CHECK_CORRECTION
          auto tmp = index.Find(ops_key[i]);
          if (0 != tmp) {
            std::cout << "Insert wrong! i:" << i << ",\tkey:" << ops_key[i]
                      << std::endl;
            std::cout << "val:" << 0 << std::endl;
            std::cout << "tmp:" << tmp << std::endl;
            index.Find(ops_key[i]);
          }
#endif
          break;
        }
        default:
          break;
      }
    }
  });
  PrintCurrentTime();
  index.PrintEachPartSize();

  std::cout << index.GetIndexName() << ", build_time:," << build_time
            << ", ms, avg_time:," << ns * 1.0 / ops_size << ", ns,"
            << " in-memory size:," << PRINT_MIB(index.GetNodeSize())
            << ", MiB, total_size:," << PRINT_MIB(index.GetTotalSize())
            << ", MiB, #ops," << ops_size << ", throughput:,"
            << ops_size * 1.0 / ns * 1e9 / 1e3 << ", K ops/s";
  std::cout << std::endl;
  std::cout << "\tres:" << res << std::endl;
}

#endif  // !UTILS_BENCHMARK_H