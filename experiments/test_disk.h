
#include <fstream>
#include <string>
#include <utility>

#include "./util.h"

#ifndef EXPERIMENTS_TEST_DISK_H_
#define EXPERIMENTS_TEST_DISK_H_

pthread_mutex_t mutex_task;

#if NUM_THREADS > 1
bool test_thread_finished = false;
double first_latency = 0;
#endif

template <typename IndexType>
static void* TestDiskCore(void* thread_params) {
  typedef typename IndexType::K_ K;
  ThreadParams<IndexType> tmp_params =
      *static_cast<ThreadParams<IndexType>*>(thread_params);
  uint64_t data_num =
      tmp_params.params.dataset_bytes_ / tmp_params.params.record_bytes_;
  const uint64_t kGapCnt = tmp_params.params.record_bytes_ / sizeof(K);
  ResultInfo<K>* res_info = new ResultInfo<K>();
  auto size = tmp_params.lookups.size();

  res_info->latency_sum = GetNsTime([&] {
    for (uint64_t i = 0; i < size; i++) {
#if NUM_THREADS > 1
      if (test_thread_finished) {
        break;
      }
#endif
      SearchRange range = {tmp_params.lookups[i].second - tmp_params.diff,
                           tmp_params.lookups[i].second + 1};
      if (tmp_params.lookups[i].second < tmp_params.diff) {
        range.start = 0;
      }
      range.stop = std::min(range.stop, data_num);

      ResultInfo<K> read_res = NormalCoreLookup(
          range, tmp_params.lookups[i].first, tmp_params.params, kGapCnt);

      res_info->total_search_range += read_res.total_search_range;
      if (read_res.total_search_range > res_info->max_search_range) {
        res_info->max_search_range = read_res.total_search_range;
      }
      res_info->res += read_res.res;
      res_info->fetch_page_num += read_res.fetch_page_num;
      res_info->total_io += read_res.total_io;
      res_info->ops++;
    }
  });

#if NUM_THREADS > 1
  pthread_mutex_lock(&mutex_task);
  if (!test_thread_finished) {
    first_latency = res_info->latency_sum * 1.0 / res_info->ops;
    test_thread_finished = true;
  }
  pthread_mutex_unlock(&mutex_task);
#endif
  return static_cast<void*>(res_info);
}

template <typename IndexType>
void TestDisk(const typename IndexType::DataVev_& data,
              const uint64_t lookup_num,
              const Params<typename IndexType::K_>& params,
              const typename IndexType::param_t diff) {
  std::cout << "\nTest Disk: " << diff << std::endl;

  auto seed = std::chrono::system_clock::now().time_since_epoch().count();
  typename IndexType::DataVev_ tmp_lookups(lookup_num);
  typename IndexType::K_ actual_res = 0;
  for (uint64_t i = 0, cnt = 0; i < lookup_num; i++) {
    if (cnt >= data.size()) {
      cnt = 0;
    }
    tmp_lookups[i] = data[cnt];
    cnt += params.record_num_per_page_ / 4;
    actual_res += tmp_lookups[i].first;
  }
  std::shuffle(tmp_lookups.begin(), tmp_lookups.end(),
               std::default_random_engine(seed));

  ResultInfo<typename IndexType::K_> res_info;
  mutex_task = PTHREAD_MUTEX_INITIALIZER;
  uint64_t ns = GetNsTime([&] {
#if NUM_THREADS == 1
    ThreadParams<IndexType> tmp_params(params, IndexType(), tmp_lookups, diff);
    res_info = *static_cast<ResultInfo<typename IndexType::K_>*>(
        TestDiskCore<IndexType>(static_cast<void*>(&tmp_params)));
    res_info.ops = lookup_num;
#else
    test_thread_finished = false;
    pthread_t thread_handles[NUM_THREADS];
    ThreadParams<IndexType> thread[NUM_THREADS];
    ResultInfo<typename IndexType::K_> retval;

    auto seg = lookup_num / NUM_THREADS;
    for (int i = 0; i < NUM_THREADS - 1; i++) {
      thread[i].lookups = typename IndexType::DataVev_(
          tmp_lookups.begin() + i * seg, tmp_lookups.begin() + (i + 1) * seg);
    }
    thread[NUM_THREADS - 1].lookups = typename IndexType::DataVev_(
        tmp_lookups.begin() + (NUM_THREADS - 1) * seg, tmp_lookups.end());

    for (int i = 0; i < NUM_THREADS; i++) {
      thread[i].params = params;
      thread[i].diff = diff;
      pthread_create(&thread_handles[i], NULL, TestDiskCore<IndexType>,
                     static_cast<void*>(&thread[i]));
    }
    for (int i = 0; i < NUM_THREADS; i++) {
      void* tmp_ret;
      pthread_join(thread_handles[i], &tmp_ret);
      retval = *static_cast<ResultInfo<typename IndexType::K_>*>(tmp_ret);
      res_info.total_search_range += retval.total_search_range;
      if (retval.max_search_range > res_info.max_search_range) {
        res_info.max_search_range = retval.max_search_range;
      }
      res_info.res += retval.res;
      res_info.fetch_page_num += retval.fetch_page_num;
      res_info.total_io += retval.total_io;
      res_info.ops += retval.ops;
      res_info.latency_sum += retval.latency_sum;
    }
#endif
  });
  pthread_mutex_destroy(&mutex_task);

#if NUM_THREADS == 1
  double latency = res_info.latency_sum * 1.0 / res_info.ops;
#else
  double latency = first_latency;
#endif

#define FAST_CHECK
#ifdef FAST_CHECK
  std::ofstream output("results/testDisk/prof_res.csv",
                       std::ios::app | std::ios::out);
  // output << "#threads, diff, fetch strategy, ops, throughput, latency\n";
  output << NUM_THREADS << ", " << diff << ", " << params.fetch_strategy_
         << ", " << res_info.ops << ", " << res_info.ops * 1.0 / ns * 1e9
         << ", " << latency << "\n";

#endif

  std::cout << "Evaluate index on disk:,DISK_" << diff << ",,,, avg_time:,"
            << ns * 1.0 / res_info.ops << ", ns,"
            << ",,, #ops," << res_info.ops << ",, avg_page:,"
            << res_info.fetch_page_num * 1.0 / res_info.ops << ", avg_range:,"
            << res_info.total_search_range * 1.0 / res_info.ops
            << ", max_range:," << res_info.max_search_range << ", pred_gran:,"
            << params.pred_granularity_ << ", fetch_strategy_:,"
            << params.fetch_strategy_;
  if (res_info.res == actual_res || res_info.ops != lookup_num) {
    std::cout << ", FIND SUCCESS,,,";
  } else {
    std::cout << ", FIND WRONG res:," << res_info.res << ", actual res:,"
              << actual_res;
  }
  std::cout << ",,,,, #threads:," << NUM_THREADS << ", throughput:,"
            << res_info.ops * 1.0 / ns * 1e9 << ", ops/sec, avg_io:,"
            << res_info.total_io * 1.0 / res_info.ops << ", total IO:,"
            << res_info.total_io << ", IOPS:,"
            << res_info.total_io * 1.0 / ns * 1e9 << ", Bandwidth:,"
            << params.page_bytes_ / 1024.0 / 1024.0 / 1024.0 *
                   res_info.fetch_page_num / ns * 1e9
            << ", GB/s, latency:," << latency << ", ns";
  std::cout << std::endl;
}

#endif  // EXPERIMENTS_TEST_DISK_H_
