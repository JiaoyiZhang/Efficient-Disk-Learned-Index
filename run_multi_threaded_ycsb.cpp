#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <vector>

#include "./key_type.h"
#include "./ycsb_utils/multi_threaded_benchmark.h"
#include "indexes/baseline/btree-mt-disk.h"
#include "indexes/multi_threaded_hybrid/dynamic/btree.h"
#include "indexes/multi_threaded_hybrid/hybrid_index.h"
#include "indexes/multi_threaded_hybrid/static/cpr_di.h"
#include "indexes/multi_threaded_hybrid/static/leco-page.h"

int main(int argc, char* argv[]) {
  char* endptr;
  if (argc < 3) {
    for (auto i = 0; i < argc; i++) {
      std::cout << i << ": " << argv[i] << std::endl;
    }
    std::cout << " Usage: " << argv[0] << std::endl
              << "  1. workload_path" << std::endl
              << "  2. is_range_scan" << std::endl
              << "  3. index_name" << std::endl
              << "  4. index_params_1" << std::endl
              << "  5. index_params_2" << std::endl
              << "  6. stored_path" << std::endl
              << "  7. page_bytes" << std::endl
              << "  8. threads_number" << std::endl
              << "  9. memory_budget/ratio (only for hybrid learned indexes)\n"
              << "  10. merging_threads_number" << std::endl;
    return -1;
  }
  const std::string kWorkloadPath = argv[1];
  const bool kRangeScan = strtoul(argv[2], &endptr, 10);
  const std::string kIndexName = argv[3];
  const float kIndexParams1 = strtof(argv[4], &endptr);
  const float kIndexParams2 = strtof(argv[5], &endptr);

  const std::string kFilepath = argv[6];
  const uint64_t kPageBytes = strtoul(argv[7], &endptr, 10);
  const uint64_t kThreadNum = strtoul(argv[8], &endptr, 10);
  uint64_t kMergeThreadNum = 1;

  // has been sorted during prepare stage
  std::cout << "\n\n--------------- LOADING ----------------" << std::endl;
  DataVec init_data = LoadKeys<Record>(kWorkloadPath + INIT_SUFFIX);
  KeyVec ops_key = LoadKeys<Key>(kWorkloadPath + OPS_KEY_SUFFIX);
  std::vector<int> ops = LoadKeys<int>(kWorkloadPath + OPS_SUFFIX);
  std::vector<int> len;
  if (kRangeScan) {
    len = LoadKeys<int>(kWorkloadPath + RANGE_LEN_SUFFIX);
  }

  std::cout << "The data in the static index is stored on disk." << std::endl;

  enum IndexName { HYBRID_BTREE_DI, HYBRID_BTREE_LECO, BTREE };

  std::map<std::string, int> index_name = {
      {"HYBRID_BTREE_DI", HYBRID_BTREE_DI},
      {"HYBRID_BTREE_LECO", HYBRID_BTREE_LECO},
      {"BTREE", BTREE}};
  typedef MultiThreadedBTreeIndex<Key, Value> Dy_BTree;
  typedef MultiThreadedStaticCprDI<Key, Value> Sta_DI;
  typedef MultiThreadedStaticLecoPage<Key, Value> Sta_Leco;
  MultiThreadedStaticLecoPage<Key, Value>::param_t leco_para;
  uint64_t fix = kIndexParams2, slide = 0;
  switch (static_cast<int>(kIndexParams2)) {
    case 2:
      fix = 0;
      slide = 1;
      break;
    case 3:
      fix = 1;
      slide = 1;
      break;
    case 4:
      fix = 0;
      slide = 2;
      break;
    case 5:
      fix = 1;
      slide = 2;
      break;
  }

  size_t memory_budget = 100;  // static / dynamic
  if (argc >= 11) {
    memory_budget = strtoul(argv[9], &endptr, 10);
    kMergeThreadNum = strtoul(argv[10], &endptr, 10);
    std::cout
        << "the memory ratio (the size of static : the size of dynamic) is:"
        << memory_budget << ",\tmerge thread num:" << kMergeThreadNum
        << std::endl;
  }

  leco_para = MultiThreadedStaticLecoPage<Key, Value>::param_t{
      kPageBytes / sizeof(Record),
      fix,
      slide,
      1000,
      {kFilepath, kPageBytes, kThreadNum, kMergeThreadNum}};

  PrintCurrentTime();

  switch (index_name[kIndexName]) {
    case HYBRID_BTREE_DI: {
      RunMultiYCSBBenchmark<
          MultiThreadedHybridIndex<Key, Value, Dy_BTree, Sta_DI>>(
          init_data, ops, ops_key, len, kThreadNum,
          {{},
           {kIndexParams2,
            kPageBytes / sizeof(Record),
            {kFilepath, kPageBytes, kThreadNum, kMergeThreadNum}},
           memory_budget});
      break;
    }
    case HYBRID_BTREE_LECO: {
      RunMultiYCSBBenchmark<
          MultiThreadedHybridIndex<Key, Value, Dy_BTree, Sta_Leco>>(
          init_data, ops, ops_key, len, kThreadNum,
          {{}, leco_para, memory_budget});
      break;
    }
    case BTREE: {
      RunMultiYCSBBenchmark<BaselineBTreeMTDisk<Key, Value>>(
          init_data, ops, ops_key, len, kThreadNum, {kFilepath});
      break;
    }
    default:
      throw std::runtime_error("The index is invalid!");
  }
}
