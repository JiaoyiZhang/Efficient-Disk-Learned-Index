#include <fstream>
#include <iostream>
#include <map>
#include <sstream>

#include "experiments/benchmark.h"
#include "indexes/DI-v1.h"
#include "indexes/DI-v3.h"
#include "indexes/DI-v4.h"
#include "indexes/binary-search.h"
#include "indexes/compressed-pgm-index.h"
#include "indexes/leco-page.h"
#include "indexes/leco-zonemap.h"
#include "indexes/pgm-index-disk-pg.h"
#include "indexes/pgm-index-disk.h"
#include "indexes/pgm-index.h"
#include "indexes/rs-disk-oriented.h"
#include "indexes/rs-disk-pg.h"
#include "indexes/rs-disk.h"

int main(int argc, char* argv[]) {
  char* endptr;
  if ((argc != 9 && argc != 14 && argc != 15) ||
      strtoul(argv[1], &endptr, 10) > 1) {
    for (auto i = 0; i < argc; i++) {
      std::cout << i << ": " << argv[i] << std::endl;
    }
    std::cout << "Parameters are invalid!" << std::endl;
    std::cout << "(a) Evaluate indexes in memory:" << std::endl;
    std::cout << " Usage: " << argv[0]
              << "  <memory_hierarchy(0)> <dataset_file> <payload_bytes> "
                 "<prediction_granularity> <number_of_lookups> <index_name> "
                 "<index_params> <first_run>"
              << std::endl;
    std::cout
        << "\tExample: ./build/LID 0 ./datasets/dataset 0 1 1000 PGM-Index 64 1"
        << std::endl;
    std::cout << "(b) Evaluate indexes on disk:" << std::endl;
    std::cout
        << " Usage: " << argv[0]
        << "  <memory_hierarchy(1)> <dataset_file> <payload_bytes> "
           "<prediction_granularity> <number_of_lookups> <index_name> "
           "<index_params> <first_run> <data_dir> <file_size_in_MB> "
           "<is_compression_mode (0: false, 1: true)> <page_size_in_KB (the "
           "mean page_size in compression setting)> \n<fetch_strategy \n\t(0: "
           "worst case, [start, end), \n\t1: one by one, [start, end), \n\t2: "
           "worst case from the middle position, (a) mid, [mid+1, end), or (b) "
           "mid, [start, mid), \n\t3: one by one from the middle "
           "position, (a) "
           "mid, mid+1, ... or (b) mid, mid-1, ...)>"
        << std::endl;
    std::cout << "\tExample: ./build/LID 1 ./datasets/dataset 0 1 1000 "
                 "PGM-Index 64 1 ./datasets/data/ 1024 0 4 1 1"
              << std::endl;
    return -1;
  }
  std::cout << "------------------------START LID-----------------------\n";

  // PrintBaseInfo();

  typedef uint64_t Key;
  typedef uint64_t Value;
  typedef std::pair<Key, Value> Record;
  const uint64_t kLookupNum = strtoul(argv[5], &endptr, 10);
  const std::string kIndexName = argv[6];
  uint64_t kIndexParams = strtoul(argv[7], &endptr, 10);
  const bool KFirstRun = strtoul(argv[8], &endptr, 10);

  std::vector<Key> keys = LoadKeys<Key>(argv[2]);
  if (!is_sorted(keys.begin(), keys.end())) {
    std::sort(keys.begin(), keys.end());
  }

  Params<Key> params(argv, keys.size());
  // params.PrintParams();
  PrintCurrentTime();
  std::cout << "# of lookup keys:, " << kLookupNum << std::endl;
  std::cout << "KFirstRun:, " << KFirstRun << std::endl;

  std::vector<Record> data(keys.size());

  if (params.is_on_disk_ && params.is_compression_mode_) {
#if ALIGNED_COMPRESSION == 0
    kIndexParams *= params.record_bytes_;
#else
    kIndexParams *= sizeof(Key) * (params.comp_block_bytes.GetMean());
#endif
    data = StoreCompressedData<Key>(keys, params);
    params.comp_block_bytes.key_num_.clear();
    for (size_t i = 0; i < data.size(); i++) {
      data[i] = {data[i].first, data[i].second / params.pred_granularity_};
    }
  } else {
    for (size_t i = 0; i < keys.size(); i++) {
      data[i] = {keys[i], i / params.pred_granularity_};
    }
  }

  if (params.is_on_disk_ && !params.is_compression_mode_ && KFirstRun) {
    StoreData<Key>(keys, params);
  }
  int file_num = std::ceil(keys.size() * 1.0 / params.record_num_per_file_);
  params.open_files = OpenFiles(params.data_dir_, file_num);
  std::cout << "\nopen " << params.open_files.size() << " files" << std::endl;

  const uint64_t lookup_num =
      kLookupNum > data.size() ? data.size() : kLookupNum;
  std::string lookup_filename = params.dataset_filename_ + "_" +
                                std::to_string(params.page_bytes_) + "_lookups";

  std::map<std::string, int> index_name = {{"PGM-Index", kPGMIndex},
                                           {"PGM-Index-Page", kPGMIndexPage},
                                           {"RadixSpline", kRadixSpline},
                                           {"PGM-PG", kPGM_PG},
                                           {"RS-PG", kRS_PG},
                                           {"RS-DISK-ORIENTED", kRS_DISK},
                                           {"CompressedPGM", kCompressedPGM},
                                           {"BinarySearch", kBinarySearch},
                                           {"DI-V1", kDIV1},
                                           {"DI-V3", kDIV3},
                                           {"DI-V4", kDIV4},
                                           {"LecoZonemap", kLecoZonemap},
                                           {"LecoPage", KLecoPage},
                                           {"DISK", kDISK}};

  LookupInfo<Key> lookup_info;
  std::vector<Record> lookups;
  if (index_name[kIndexName] < index_name["DISK"]) {
    lookups =
        GetLookupKeys<Key, Value>(lookup_filename, data, lookup_num, KFirstRun,
                                  params.is_on_disk_, &lookup_info);
    std::cout << "get lookup keys over!" << std::endl;
  }

  switch (index_name[kIndexName]) {
    case kPGMIndex: {
      if (params.is_compression_mode_) {
        throw std::runtime_error("PGMIndex does not support compression mode!");
      }
      switch (kIndexParams) {
        case 128:
          Evaluate<PGMIndex<Key, Value, 128>>(data, lookups, lookup_info,
                                              params, params.pred_granularity_);
          break;
        case 256:
          Evaluate<PGMIndex<Key, Value, 256>>(data, lookups, lookup_info,
                                              params, params.pred_granularity_);
          break;
        default:
          Evaluate<PGMIndex<Key, Value, 64>>(data, lookups, lookup_info, params,
                                             params.pred_granularity_);
          break;
      }
      break;
    }
    case kPGMIndexPage:
      Evaluate<PGMIndexPage<Key, Value>>(data, lookups, lookup_info, params,
                                         kIndexParams);
      break;
    case kRadixSpline:
      Evaluate<RSIndex<Key, Value>>(data, lookups, lookup_info, params,
                                    {12, kIndexParams});
      break;
    case kPGM_PG:
      Evaluate<PGMIndexPagePG<Key, Value>>(
          data, lookups, lookup_info, params,
          {kIndexParams, params.pred_granularity_});
      break;
    case kRS_PG:
      Evaluate<RSPGIndex<Key, Value>>(
          data, lookups, lookup_info, params,
          {12, kIndexParams, params.pred_granularity_});
      break;
    case kRS_DISK:
      Evaluate<RSDiskIndex<Key, Value>>(
          data, lookups, lookup_info, params,
          {12, kIndexParams, params.record_num_per_page_});
      break;
    case kCompressedPGM:
      Evaluate<CompressedPGM<Key, Value>>(data, lookups, lookup_info, params,
                                          kIndexParams);
      break;
    case kBinarySearch:
      Evaluate<BinarySearch<Key, Value>>(data, lookups, lookup_info, params,
                                         kIndexParams);
      break;
    case kDIV1: {
      float lambda = strtof(argv[7], &endptr);
      Evaluate<DI_V1<Key, Value>>(data, lookups, lookup_info, params,
                                  {lambda, params.record_num_per_page_});

      break;
    }
    case kDIV3: {
      float lambda = strtof(argv[7], &endptr);
      Evaluate<DI_V3<Key, Value>>(data, lookups, lookup_info, params,
                                  {lambda, params.record_num_per_page_});
      break;
    }
    case kDIV4: {
      float lambda = strtof(argv[7], &endptr);
      Evaluate<DI_V4<Key, Value>>(data, lookups, lookup_info, params,
                                  {lambda, params.record_num_per_page_});
      break;
    }
    case kLecoZonemap: {
      int total_pages = kIndexParams / params.record_num_per_page_;
      if (argc == 15) {
        std::string dataname = argv[14];
        auto p = GetLecoParams<LecoZonemap<Key, Value>>(
            total_pages, params.record_num_per_page_, dataname);
        std::cout << "\n\nnow test leco on: block_num:" << p.block_num_
                  << std::endl;
        Evaluate<LecoZonemap<Key, Value>>(data, lookups, lookup_info, params,
                                          p);

      } else {
        size_t min_size = UINT64_MAX;
        size_t min_block;
        std::vector<size_t> block_num = {
            700,  800,  900,  1000, 1100, 1200, 1250, 1300, 1400,
            1500, 1600, 1700, 1800, 1900, 2000, 2100, 2200, 2300,
            2400, 2500, 2600, 2700, 2800, 2900, 3000, 3500, 4000,
            4500, 5000, 6000, 7000, 8000, 9000, 10000};
        for (auto b : block_num) {
          std::cout << "\n\nnow test leco on: block_num:" << b << std::endl;
          auto res = Evaluate<LecoZonemap<Key, Value>>(
              data, lookups, lookup_info, params,
              {params.record_num_per_page_, total_pages, b});
          if (res < min_size) {
            min_size = res;
            min_block = b;
          }
        }
        std::cout << "the minimum size is:" << min_size / 1024.0 / 1024.0
                  << ",\tb:" << min_block << std::endl;
      }
      break;
    }
    case KLecoPage: {
      int total_pages = kIndexParams / params.record_num_per_page_;
      if (argc == 15) {
        std::string dataname = argv[14];
        auto p = GetLecoPageParams<LecoPage<Key, Value>>(
            total_pages, params.record_num_per_page_, dataname);
        if (p.block_num_ == 0) {
          std::vector<size_t> fix_list;
          for (int i = 0; i <= total_pages; i++) {
            fix_list.push_back(i);
          }
          std::vector<size_t> slide_list;
          for (int i = 0; i <= total_pages / 2; i++) {
            slide_list.push_back(i);
          }
          size_t min_size = UINT64_MAX;
          size_t min_fix, min_slide;

          for (auto s : slide_list) {
            for (auto f : fix_list) {
              if (f + s * 2 == total_pages) {
                std::cout << "\n\nnow test leco-page on: #slide_pages:" << s
                          << ",\t#fixed_pages:" << f << ",\tblock_num:" << 1000
                          << std::endl;
                auto res = Evaluate<LecoPage<Key, Value>>(
                    data, lookups, lookup_info, params,
                    {params.record_num_per_page_, f, s, 1000});
                if (res < min_size) {
                  min_size = res;
                  min_fix = f;
                  min_slide = s;
                }
              }
            }
          }
        } else {
          std::cout << "\n\nnow test leco-page on: #slide_pages:"
                    << p.slide_page_ << ",\t#fixed_pages:" << p.fix_page_
                    << ",\tblock_num:" << p.block_num_ << std::endl;
          Evaluate<LecoPage<Key, Value>>(data, lookups, lookup_info, params, p);
        }

      } else {
        std::vector<size_t> fix_list;
        for (int i = 0; i <= total_pages; i++) {
          fix_list.push_back(i);
        }
        std::vector<size_t> slide_list;
        for (int i = 0; i <= total_pages / 2; i++) {
          slide_list.push_back(i);
        }
        std::vector<size_t> block_num = {
            700,  800,  900,  1000, 1100, 1200, 1250, 1300, 1400,
            1500, 1600, 1700, 1800, 1900, 2000, 2100, 2200, 2300,
            2400, 2500, 2600, 2700, 2800, 2900, 3000, 3500, 4000,
            4500, 5000, 6000, 7000, 8000, 9000, 10000};
        size_t min_size = UINT64_MAX;
        size_t min_fix, min_slide, min_block;

        for (auto s : slide_list) {
          for (auto f : fix_list) {
            for (auto b : block_num) {
              if (f + s * 2 == total_pages) {
                std::cout << "\n\nnow test leco-page on: #slide_pages:" << s
                          << ",\t#fixed_pages:" << f << ",\tblock_num:" << b
                          << std::endl;
                auto res = Evaluate<LecoPage<Key, Value>>(
                    data, lookups, lookup_info, params,
                    {params.record_num_per_page_, f, s, b});
                if (res < min_size) {
                  min_size = res;
                  min_fix = f;
                  min_slide = s;
                  min_block = b;
                }
              }
            }
          }
        }
        std::cout << "the minimum size is:" << min_size / 1024.0 / 1024.0
                  << ",\tfix:" << min_fix << ",\ts:" << min_slide
                  << ",\tb:" << min_block << std::endl;
      }
      break;
    }
    case kDISK:
      TestDisk<BinarySearch<Key, Value>>(data, lookup_num, params,
                                         kIndexParams);
      break;
    default:
      throw std::runtime_error("The index is invalid!");
  }
  for (auto it = params.open_files.begin(); it != params.open_files.end();
       it++) {
    auto fd = it->second;
    DirectIOClose(fd);
  }
  free(params.read_buf_);
}
