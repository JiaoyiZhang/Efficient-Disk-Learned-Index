#include <string>
#include <utility>

#include "./test_disk.h"
#include "./util.h"

#ifndef EXPERIMENTS_BENCHMARK_H_
#define EXPERIMENTS_BENCHMARK_H_

template <typename IndexType>
PageStats CalculatePageStats(typename IndexType::DataVev_& data,
                             IndexType index,
                             const Params<typename IndexType::K_>& params) {
  PageStats stats;
  uint64_t page_sum = 0, dist_sum = 0, mid_sum = 0;
  auto size = data.size() - 1;
  std::vector<double> diff;
  int last_idx = 0;
  int cnt = 0;
  for (uint64_t i = 0; i < size; i++) {
    if (i > 0 && data[i].first == data[last_idx].first) {
      diff.push_back(diff.back());
      cnt++;
      continue;
    }
    last_idx = i;
    SearchRange range = index.Lookup(data[i].first);
    GetItemRange(&range, params.pred_granularity_, data.size());

    FetchRange fetch_range =
        GetFetchRange<typename IndexType::K_>(params, range);
    auto pid_start = fetch_range.pid_start;
    auto pid_end = fetch_range.pid_end;
    page_sum += (pid_end - pid_start + 1);

    auto dist_pid = i / params.record_num_per_page_;
    dist_sum += (dist_pid - pid_start + 1);

    uint64_t mid_pid = (range.start + range.stop) / 2;
    diff.push_back(i * 1.0 - mid_pid * 1.0);
    mid_pid /= params.record_num_per_page_;
    if (dist_pid < mid_pid) {
      mid_sum += (mid_pid - dist_pid + 1);
    } else {
      mid_sum += (dist_pid - mid_pid + 1);
    }
  }

#ifdef PRINT_INFO
  std::ofstream hist_file(
      params.dataset_filename_ + "_" + index.GetIndexName() + "_hist.bin",
      std::ios::binary);
  hist_file.write((char*)&diff[0], diff.size() * sizeof(double));
  hist_file.close();
#endif

  stats.avg_dist = dist_sum * 1.0 / (size - cnt);
  stats.avg_mid = mid_sum * 1.0 / (size - cnt);
  stats.avg_page = page_sum * 1.0 / (size - cnt);
  return stats;
}

template <typename IndexType>
typename IndexType::param_t GetLecoParams(size_t total_pages,
                                          uint64_t record_per_page,
                                          std::string dataset) {
  // struct param_t {
  //   size_t record_per_page_;
  //   size_t tolerance_;
  //   size_t block_num_;
  // };
  typename IndexType::param_t p = {record_per_page, total_pages, 1000};
  if (dataset == "fb_200M_uint64") {
    switch (total_pages) {
      case 1: {
        p = {record_per_page, total_pages, 2100};
        break;
      }
      case 2: {
        p = {record_per_page, total_pages, 3500};
        break;
      }
      case 3: {
        p = {record_per_page, total_pages, 2400};
        break;
      }
      case 4: {
        p = {record_per_page, total_pages, 1600};
        break;
      }
      case 5: {
        p = {record_per_page, total_pages, 1250};
        break;
      }
      default:
        break;
    }
  } else if (dataset == "books_200M_uint64") {
    switch (total_pages) {
      case 1: {
        p = {record_per_page, total_pages, 8000};
        break;
      }
      case 2: {
        p = {record_per_page, total_pages, 2500};
        break;
      }
      case 3: {
        p = {record_per_page, total_pages, 1900};
        break;
      }
      case 4: {
        p = {record_per_page, total_pages, 1250};
        break;
      }
      case 5: {
        p = {record_per_page, total_pages, 800};
        break;
      }
      default:
        break;
    }
  } else if (dataset == "wiki_ts_200M_uint64") {
    switch (total_pages) {
      case 1: {
        p = {record_per_page, total_pages, 7000};
        break;
      }
      case 2: {
        p = {record_per_page, total_pages, 10000};
        break;
      }
      case 3: {
        p = {record_per_page, total_pages, 4500};
        break;
      }
      case 4: {
        p = {record_per_page, total_pages, 2800};
        break;
      }
      case 5: {
        p = {record_per_page, total_pages, 700};
        break;
      }
      default:
        break;
    }
  } else if (dataset == "osm_cellids_200M_uint64") {
    switch (total_pages) {
      case 1: {
        p = {record_per_page, total_pages, 8000};
        break;
      }
      case 2: {
        p = {record_per_page, total_pages, 4000};
        break;
      }
      case 3: {
        p = {record_per_page, total_pages, 3500};
        break;
      }
      case 4: {
        p = {record_per_page, total_pages, 2500};
        break;
      }
      case 5: {
        p = {record_per_page, total_pages, 1700};
        break;
      }
      default:
        break;
    }
  }
  return p;
}

template <typename IndexType>
typename IndexType::param_t GetLecoPageParams(size_t total_pages,
                                              uint64_t record_per_page,
                                              std::string dataset) {
  // struct param_t {
  //   size_t record_per_page_;
  //   size_t fix_page_;
  //   size_t slide_page_;
  //   size_t block_num_;
  // };
  typename IndexType::param_t p = {record_per_page, 0, 0, 0};
  if (dataset == "fb_200M_uint64") {
    switch (total_pages) {
      case 1: {
        p = {record_per_page, 1, 0, 2100};
        break;
      }
      case 2: {
        p = {record_per_page, 0, 1, 3500};
        break;
      }
      case 3: {
        p = {record_per_page, 1, 1, 2400};
        break;
      }
      case 4: {
        p = {record_per_page, 2, 1, 1600};
        break;
      }
      case 5: {
        p = {record_per_page, 3, 1, 1250};
        break;
      }
      default:
        break;
    }
  } else if (dataset == "books_200M_uint64") {
    switch (total_pages) {
      case 1: {
        p = {record_per_page, 1, 0, 8000};
        break;
      }
      case 2: {
        p = {record_per_page, 0, 1, 2500};
        break;
      }
      case 3: {
        p = {record_per_page, 1, 1, 1900};
        break;
      }
      case 4: {
        p = {record_per_page, 0, 2, 1250};
        break;
      }
      case 5: {
        p = {record_per_page, 1, 2, 800};
        break;
      }
      default:
        break;
    }
  } else if (dataset == "wiki_ts_200M_uint64") {
    switch (total_pages) {
      case 1: {
        p = {record_per_page, 1, 0, 7000};
        break;
      }
      case 2: {
        p = {record_per_page, 0, 1, 10000};
        break;
      }
      case 3: {
        p = {record_per_page, 1, 1, 4500};
        break;
      }
      case 4: {
        p = {record_per_page, 2, 1, 2800};
        break;
      }
      case 5: {
        p = {record_per_page, 3, 1, 700};
        break;
      }
      default:
        break;
    }
  } else if (dataset == "osm_cellids_200M_uint64") {
    switch (total_pages) {
      case 1: {
        p = {record_per_page, 1, 0, 8000};
        break;
      }
      case 2: {
        p = {record_per_page, 2, 0, 4000};
        break;
      }
      case 3: {
        p = {record_per_page, 1, 1, 3500};
        break;
      }
      case 4: {
        p = {record_per_page, 2, 1, 2500};
        break;
      }
      case 5: {
        p = {record_per_page, 3, 1, 1700};
        break;
      }
      default:
        break;
    }
  } else {
    std::cout << "Please remove the last parameter to tune the leco-page!"
              << std::endl;
  }
  return p;
}

template <typename IndexType>
size_t Evaluate(typename IndexType::DataVev_& data,
                const typename IndexType::DataVev_& lookups,
                const LookupInfo<typename IndexType::V_>& lookup_info,
                const Params<typename IndexType::K_>& params,
                const typename IndexType::param_t index_params) {
  IndexType index(index_params);
  const uint64_t build_time = GetNsTime([&] { index.Build(data); }) / 1e6;

  std::cout << "\nBuild index: " << index.GetIndexName() << " over"
            << std::endl;
  std::cout << "\nGetModelNum of, " << index.GetIndexName() << ", pred_gran:,"
            << params.pred_granularity_;
  if (params.pred_granularity_ > 2) {
    std::cout << ", #training data:,"
              << data.size() / params.pred_granularity_ * 2;
  } else {
    std::cout << ", #training data:," << data.size();
  }
  std::cout << ", build_time/ms:," << build_time << ", #model:,"
            << index.GetModelNum() << ", space/MiB:,"
            << index.GetInMemorySize() / 1024.0 / 1024.0 << "\n"
            << std::endl;

#ifdef PRINT_PAGE_STATS
  auto stats = CalculatePageStats<IndexType>(data, index, params);
  std::cout << "avg pages of 1 by 1:" << stats.avg_dist << std::endl
            << "avg pages of worst case:" << stats.avg_page << std::endl
            << "avg pages of middle case:" << stats.avg_mid << std::endl
            << "res:" << stats.avg_dist << ", " << stats.avg_page << ", "
            << stats.avg_mid << std::endl;
#endif  // PRINT_PAGE_STATS

  auto seed = std::chrono::system_clock::now().time_since_epoch().count();
  auto tmp_lookups = lookups;
  std::shuffle(tmp_lookups.begin(), tmp_lookups.end(),
               std::default_random_engine(seed));

  ResultInfo<typename IndexType::K_> res_info;
  uint64_t ns;
  if (params.is_on_disk_) {
#ifdef TEST_SEARCH
    const uint64_t kGapCnt =
        params.record_bytes_ / sizeof(typename IndexType::K_);
    for (uint64_t i = 0; i < data.size(); i++) {
      SearchRange range = index.Lookup(data[i].first);
      GetItemRange(&range, params.pred_granularity_, data.size());
      ResultInfo<typename IndexType::K_> read_res;
      if (!params.is_compression_mode_) {
        read_res = NormalCoreLookup(range, data[i].first, params, kGapCnt);
      } else {
        read_res = CompressionCoreLookup(range, data[i].first, params, kGapCnt);
      }
      if (read_res.res != data[i].first) {
        std::cout << "FIND " << i << " WRONG! key:" << data[i].first
                  << ",\tv:" << data[i].second
                  << ",\tres.first:" << read_res.res << std::endl;
        std::cout << "range.start: " << range.start << ",\tstop:" << range.stop
                  << std::endl;
        range = index.Lookup(data[i].first);
        if (!params.is_compression_mode_) {
          read_res = NormalCoreLookup(range, data[i].first, params, kGapCnt);
        } else {
          read_res =
              CompressionCoreLookup(range, data[i].first, params, kGapCnt);
        }
      }
    }
    std::cout << "TEST ON-DISK SEARCH OVER" << std::endl;
#endif
    ns = GetNsTime(
        [&] { res_info = DoLookups<IndexType>(index, tmp_lookups, params); });

    std::cout << "Evaluate index on disk:,";
  } else {
#ifdef TEST_SEARCH
    for (uint64_t i = 0; i < data.size(); i++) {
      SearchRange range = index.Lookup(data[i].first);
      GetItemRange(&range, params.pred_granularity_, data.size());

      auto it = std::lower_bound(
          data.begin() + range.start, data.begin() + range.stop, data[i].first,
          [](const auto& lhs, const typename IndexType::K_ key) {
            return lhs.first < key;
          });

      if (it->first != data[i].first) {
        std::cout << "FIND " << i << " WRONG! key:" << data[i].first
                  << ",\tv:" << data[i].second << ",\tres.first:" << it->first
                  << std::endl;
        auto tmp_range = index.Lookup(data[i].first);
        std::cout << "tmp_range.start: " << tmp_range.start
                  << ",\tstop:" << tmp_range.stop << std::endl;
      }
    }
    std::cout << "TEST IN-MEMORY SEARCH OVER" << std::endl;
#endif
    ns = GetNsTime([&] {
      res_info = DoMemoryLookups<IndexType>(index, data, tmp_lookups,
                                            params.pred_granularity_);
    });

    std::cout << "Evaluate index in memory:,";
  }
  std::cout << index.GetIndexName() << ", build_time:," << build_time
            << ", ms, avg_time:," << ns * 1.0 / res_info.ops << ", ns,"
            << " in-memory_size:," << index.GetInMemorySize() / 1024.0 / 1024.0
            << ", MiB, #ops," << res_info.ops << ",, avg_page:,"
            << res_info.fetch_page_num * 1.0 / res_info.ops << ", avg_range:,"
            << res_info.total_search_range * 1.0 / res_info.ops
            << ", max_range:," << res_info.max_search_range << ", pred_gran:,"
            << params.pred_granularity_ << ", fetch_strategy_:,"
            << params.fetch_strategy_;
  if (res_info.res == lookup_info.actual_res ||
      res_info.ops != tmp_lookups.size()) {
    std::cout << ", FIND SUCCESS,,,";
  } else {
    std::cout << ", FIND WRONG res:," << res_info.res << ", actual res:,"
              << lookup_info.actual_res;
  }
  if (params.is_on_disk_) {
    std::cout << ",,,,";
  } else {
    std::cout << ", avg_len:," << lookup_info.total_len * 1.0 / res_info.ops
              << ", max_len:," << lookup_info.max_len;
  }

#ifdef PROF_CPU_IO
  res_info.cpu_time +=
      res_info.index_predict_time + prof_file_cpu_time + last_mile_search_time;
  res_info.io_time += prof_io_time;
#endif  // PROF_CPU_IO

  std::cout << ", #threads:," << NUM_THREADS << ", throughput:,"
            << res_info.ops * 1.0 / ns * 1e9 << ", ops/sec, avg_io:,"
            << res_info.total_io * 1.0 / res_info.ops << ", total IO:,"
            << res_info.total_io << ", IOPS:,"
            << res_info.total_io * 1.0 / ns * 1e9 << ", Bandwidth:,"
            << params.page_bytes_ * 1.0 * res_info.fetch_page_num / ns
            << ", GB/s, latency:," << res_info.latency_sum
            << ", ns, predict time:,"
            << res_info.index_predict_time * 1.0 / res_info.ops << ", ns,"
            << " directIO file cpu time:,"
            << prof_file_cpu_time * 1.0 / res_info.ops << ", ns,"
            << " last-mile search cpu time:,"
            << last_mile_search_time * 1.0 / res_info.ops << ", ns,"
            << " cpu total time:," << res_info.cpu_time * 1.0 / res_info.ops
            << ", ns,"
            << " io time:," << res_info.io_time * 1.0 / res_info.ops << ", ns,";
  std::cout << std::endl;
  return index.GetInMemorySize();
}

#endif  // EXPERIMENTS_BENCHMARK_H_
