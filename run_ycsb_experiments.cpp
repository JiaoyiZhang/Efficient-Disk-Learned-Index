#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <vector>

#include "./key_type.h"
#include "./ycsb_utils/benchmark.h"
#include "indexes/baseline/btree-disk.h"
#include "indexes/baseline/film.h"
#include "indexes/baseline/pgm-disk-origin.h"
#include "indexes/hybrid/dynamic/alex.h"
#include "indexes/hybrid/dynamic/btree.h"
#include "indexes/hybrid/dynamic/pgm.h"
#include "indexes/hybrid/hybrid_index.h"
#include "indexes/hybrid/static/cpr_di.h"
#include "indexes/hybrid/static/leco-page.h"
#include "indexes/hybrid/static/pgm.h"
#include "indexes/hybrid/static/rs.h"

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
              << "  6. stored_path (on-disk mode)" << std::endl
              << "  7. page_bytes (on-disk mode)" << std::endl
              << "  8. memory_budget (only for hybrid learned indexes)"
              << "  9. buffer_ratio (only for hybrid learned indexes)"
              << std::endl;
    return -1;
  }
  const std::string kWorkloadPath = argv[1];
  const bool kRangeScan = strtoul(argv[2], &endptr, 10);
  const std::string kIndexName = argv[3];
  const float kIndexParams1 = strtof(argv[4], &endptr);
  const float kIndexParams2 = strtof(argv[5], &endptr);

  const std::string kFilepath = argv[6];
  const uint64_t kPageBytes = strtoul(argv[7], &endptr, 10);
  read_buf_ = reinterpret_cast<Key*>(
      aligned_alloc(kPageBytes, kPageBytes * ALLOCATED_BUF_SIZE));

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

  enum IndexName {
    HYBRID_ALEX_RS,
    HYBRID_ALEX_PGM,
    HYBRID_ALEX_DI,
    HYBRID_ALEX_LECO,
    HYBRID_BTREE_RS,
    HYBRID_BTREE_PGM,
    HYBRID_BTREE_DI,
    HYBRID_BTREE_LECO,
    HYBRID_PGM_RS,
    HYBRID_PGM_PGM,
    HYBRID_PGM_DI,
    HYBRID_PGM_LECO,
    PGM,
    PGM_ORIGIN,
    ALEX,
    FITING_TREE,
    BTREE,
    FILM
  };

  std::map<std::string, int> index_name = {
      {"HYBRID_ALEX_RS", HYBRID_ALEX_RS},
      {"HYBRID_BTREE_RS", HYBRID_BTREE_RS},
      {"HYBRID_PGM_RS", HYBRID_PGM_RS},
      {"HYBRID_ALEX_PGM", HYBRID_ALEX_PGM},
      {"HYBRID_BTREE_PGM", HYBRID_BTREE_PGM},
      {"HYBRID_PGM_PGM", HYBRID_PGM_PGM},
      {"HYBRID_ALEX_DI", HYBRID_ALEX_DI},
      {"HYBRID_BTREE_DI", HYBRID_BTREE_DI},
      {"HYBRID_PGM_DI", HYBRID_PGM_DI},
      {"HYBRID_ALEX_LECO", HYBRID_ALEX_LECO},
      {"HYBRID_BTREE_LECO", HYBRID_BTREE_LECO},
      {"HYBRID_PGM_LECO", HYBRID_PGM_LECO},
      {"ALEX", ALEX},
      {"BTREE", BTREE},
      {"FILM", FILM},
      {"FITING_TREE", FITING_TREE},
      {"PGM_ORIGIN", PGM_ORIGIN},
      {"PGM", PGM}};
  typedef AlexIndex<Key, Value> Dy_ALEX;
  typedef BTreeIndex<Key, Value> Dy_BTree;
  typedef RSIndex<Key, Value> Sta_RS;
  typedef DynamicPGMIndex<Key, Value> Dy_PGM;
  typedef StaticPGMIndex<Key, Value> Sta_PGM;
  typedef StaticCprDI<Key, Value> Sta_DI;
  typedef StaticLecoPage<Key, Value> Sta_Leco;
  StaticLecoPage<Key, Value>::param_t leco_para;
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
  leco_para = StaticLecoPage<Key, Value>::param_t{
      kPageBytes / sizeof(Record), fix, slide, 1000, {kFilepath, kPageBytes}};

  size_t memory_budget = 100;
  if (argc >= 9) {
    memory_budget = strtoul(argv[8], &endptr, 10);
    std::cout << "the memory budget is:" << memory_budget << " bytes, "
              << PRINT_MIB(memory_budget) << " MiB" << std::endl;
  }
  PrintCurrentTime();

  switch (index_name[kIndexName]) {
    case HYBRID_ALEX_RS: {
      RunYCSBBenchmark<HybridIndex<Key, Value, Dy_ALEX, Sta_RS>>(
          init_data, ops, ops_key, len,
          {{},
           {12, static_cast<uint64_t>(kIndexParams2), {kFilepath, kPageBytes}},
           memory_budget});
      break;
    }
    case HYBRID_BTREE_RS: {
      RunYCSBBenchmark<HybridIndex<Key, Value, Dy_BTree, Sta_RS>>(
          init_data, ops, ops_key, len,
          {{},
           {12, static_cast<uint64_t>(kIndexParams2), {kFilepath, kPageBytes}},
           memory_budget});
      break;
    }
    case HYBRID_PGM_RS: {
      RunYCSBBenchmark<HybridIndex<Key, Value, Dy_PGM, Sta_RS>>(
          init_data, ops, ops_key, len,
          {{},
           {12, static_cast<uint64_t>(kIndexParams2), {kFilepath, kPageBytes}},
           memory_budget});
      break;
    }
    case HYBRID_ALEX_PGM: {
      RunYCSBBenchmark<HybridIndex<Key, Value, Dy_ALEX, Sta_PGM>>(
          init_data, ops, ops_key, len,
          {{},
           {static_cast<uint64_t>(kIndexParams2), {kFilepath, kPageBytes}},
           memory_budget});
      break;
    }
    case HYBRID_BTREE_PGM: {
      RunYCSBBenchmark<HybridIndex<Key, Value, Dy_BTree, Sta_PGM>>(
          init_data, ops, ops_key, len,
          {{},
           {static_cast<uint64_t>(kIndexParams2), {kFilepath, kPageBytes}},
           memory_budget});
      break;
    }
    case HYBRID_PGM_PGM: {
      RunYCSBBenchmark<HybridIndex<Key, Value, Dy_PGM, Sta_PGM>>(
          init_data, ops, ops_key, len,
          {{},
           {static_cast<uint64_t>(kIndexParams2), {kFilepath, kPageBytes}},
           memory_budget});
      break;
    }
    case HYBRID_ALEX_DI: {
      RunYCSBBenchmark<HybridIndex<Key, Value, Dy_ALEX, Sta_DI>>(
          init_data, ops, ops_key, len,
          {{},
           {kIndexParams2,
            kPageBytes / sizeof(Record),
            {kFilepath, kPageBytes}},
           memory_budget});
      break;
    }
    case HYBRID_BTREE_DI: {
      RunYCSBBenchmark<HybridIndex<Key, Value, Dy_BTree, Sta_DI>>(
          init_data, ops, ops_key, len,
          {{},
           {kIndexParams2,
            kPageBytes / sizeof(Record),
            {kFilepath, kPageBytes}},
           memory_budget});
      break;
    }
    case HYBRID_PGM_DI: {
      RunYCSBBenchmark<HybridIndex<Key, Value, Dy_PGM, Sta_DI>>(
          init_data, ops, ops_key, len,
          {{},
           {kIndexParams2,
            kPageBytes / sizeof(Record),
            {kFilepath, kPageBytes}},
           memory_budget});
      break;
    }
    case HYBRID_ALEX_LECO: {
      RunYCSBBenchmark<HybridIndex<Key, Value, Dy_ALEX, Sta_Leco>>(
          init_data, ops, ops_key, len, {{}, leco_para, memory_budget});
      break;
    }
    case HYBRID_BTREE_LECO: {
      RunYCSBBenchmark<HybridIndex<Key, Value, Dy_BTree, Sta_Leco>>(
          init_data, ops, ops_key, len, {{}, leco_para, memory_budget});
      break;
    }
    case HYBRID_PGM_LECO: {
      RunYCSBBenchmark<HybridIndex<Key, Value, Dy_PGM, Sta_Leco>>(
          init_data, ops, ops_key, len, {{}, leco_para, memory_budget});
      break;
    }
    case BTREE: {
      RunYCSBBenchmark<BaselineBTreeDisk<Key, Value>>(
          init_data, ops, ops_key, len,
          {static_cast<size_t>(kIndexParams1), kFilepath});
      break;
    }
    case FILM: {
      RunYCSBBenchmark<BaselineFILM<Key, Value>>(
          init_data, ops, ops_key, len,
          {128, init_data.size(), kPageBytes, static_cast<size_t>(kIndexParams1),
           kIndexParams2, kFilepath});
      break;
    }
    case PGM_ORIGIN: {
      RunYCSBBenchmark<BaselinePGMDiskOrigin<Key, Value>>(
          init_data, ops, ops_key, len,
          {static_cast<size_t>(kIndexParams1), kFilepath});
      break;
    }
    default:
      throw std::runtime_error("The index is invalid!");
  }
  free(read_buf_);
}
