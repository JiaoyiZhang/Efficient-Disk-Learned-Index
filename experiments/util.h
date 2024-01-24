#ifndef EXPERIMENTS_UTIL_H_
#define EXPERIMENTS_UTIL_H_

#include "util_compression.h"
#include "util_lid.h"
#include "util_same_block_size.h"

pthread_mutex_t mutex;

#if NUM_THREADS > 1
bool thread_finished = false;
double first_thread_latency = 0;
#endif

/**
 * @brief Return the range in item-level: [start, stop)
 */
inline void GetItemRange(SearchRange* range, uint64_t pred_gran,
                         uint64_t data_size) {
  range->start = std::min<size_t>(range->start * pred_gran, data_size - 1);
  range->stop = std::min<size_t>(range->stop * pred_gran, data_size);
}

template <typename IndexType>
static void* DoCoreLookups(void* thread_params) {
  typedef typename IndexType::K_ K;
  ThreadParams<IndexType> tmp_params =
      *static_cast<ThreadParams<IndexType>*>(thread_params);
  uint64_t data_num =
      tmp_params.params.dataset_bytes_ / tmp_params.params.record_bytes_;
  const uint64_t kGapCnt = tmp_params.params.record_bytes_ / sizeof(K);
  ResultInfo<K>* res_info = new ResultInfo<K>;
  auto size = tmp_params.lookups.size();

  res_info->latency_sum = GetNsTime([&] {
    for (uint64_t i = 0; i < size; i++) {
#if NUM_THREADS > 1
      if (thread_finished) {
        break;
      }
#endif
      SearchRange range;
      res_info->index_predict_time += GetNsTime([&] {
        range = tmp_params.index.Lookup(tmp_params.lookups[i].first);
      });

      ResultInfo<K> read_res;
#ifdef PROF_CPU_IO
      res_info->cpu_time += GetNsTime([&] {
#endif  // PROF_CPU_IO
        GetItemRange(&range, tmp_params.params.pred_granularity_, data_num);
#ifdef PROF_CPU_IO
      });
#endif  // PROF_CPU_IO
      if (!tmp_params.params.is_compression_mode_) {
        if (tmp_params.params.pred_granularity_ > 1) {
          range.stop--;
        }
        read_res = NormalCoreLookup(range, tmp_params.lookups[i].first,
                                    tmp_params.params, kGapCnt);
      } else {
        read_res = CompressionCoreLookup(range, tmp_params.lookups[i].first,
                                         tmp_params.params, kGapCnt);
      }
      res_info->total_search_range += read_res.total_search_range;
      if (read_res.total_search_range > res_info->max_search_range) {
        res_info->max_search_range = read_res.total_search_range;
      }
      res_info->res += read_res.res;
      res_info->fetch_page_num += read_res.fetch_page_num;
      res_info->total_io += read_res.total_io;
      res_info->cpu_time += read_res.cpu_time;
      res_info->io_time += read_res.io_time;
      res_info->ops++;
    }
  });

#if NUM_THREADS > 1
  pthread_mutex_lock(&mutex);
  if (!thread_finished) {
    first_thread_latency = res_info->latency_sum * 1.0 / res_info->ops;
    thread_finished = true;
  }
  pthread_mutex_unlock(&mutex);
#endif
  return static_cast<void*>(res_info);
}

template <typename IndexType>
static inline ResultInfo<typename IndexType::K_> DoLookups(
    const IndexType& index, const typename IndexType::DataVev_& lookups,
    const Params<typename IndexType::K_>& params) {
  typedef typename IndexType::K_ K;
  ResultInfo<K> res_info;
  mutex = PTHREAD_MUTEX_INITIALIZER;
  uint64_t size = lookups.size();
#if NUM_THREADS <= 1
  ThreadParams<IndexType> tmp_params(params, index, lookups,
                                     typename IndexType::param_t());
  res_info = *static_cast<ResultInfo<K>*>(
      DoCoreLookups<IndexType>(static_cast<void*>(&tmp_params)));
  res_info.ops = size;
#else
  thread_finished = false;
  pthread_t thread_handles[NUM_THREADS];
  ThreadParams<IndexType> thread[NUM_THREADS];
  ResultInfo<K> retval;

  auto seg = size / NUM_THREADS;
  for (int i = 0; i < NUM_THREADS - 1; i++) {
    thread[i].lookups = typename IndexType::DataVev_(
        lookups.begin() + i * seg, lookups.begin() + (i + 1) * seg);
  }
  thread[NUM_THREADS - 1].lookups = typename IndexType::DataVev_(
      lookups.begin() + (NUM_THREADS - 1) * seg, lookups.end());

  for (int i = 0; i < NUM_THREADS; i++) {
    thread[i].index = index;
    thread[i].params = params;
    thread[i].params.alloc();
    thread[i].params.open_files =
        OpenFiles(params.data_dir_, params.open_files.size());
    pthread_create(&thread_handles[i], NULL, DoCoreLookups<IndexType>,
                   static_cast<void*>(&thread[i]));
  }
  for (int i = 0; i < NUM_THREADS; i++) {
    void* tmp_ret;
    pthread_join(thread_handles[i], &tmp_ret);
    retval = *static_cast<ResultInfo<K>*>(tmp_ret);
    res_info.total_search_range += retval.total_search_range;
    if (retval.max_search_range > res_info.max_search_range) {
      res_info.max_search_range = retval.max_search_range;
    }
    res_info.res += retval.res;
    res_info.fetch_page_num += retval.fetch_page_num;
    res_info.total_io += retval.total_io;
    res_info.ops += retval.ops;
    res_info.latency_sum += retval.latency_sum;
    res_info.index_predict_time += retval.index_predict_time;
  }
#endif
  pthread_mutex_destroy(&mutex);
#if NUM_THREADS == 1
  res_info.latency_sum = res_info.latency_sum * 1.0 / res_info.ops;
#else
  res_info.latency_sum = first_thread_latency;
#endif

  return res_info;
}

template <typename IndexType>
static inline ResultInfo<typename IndexType::K_> DoMemoryLookups(
    const IndexType& index, const typename IndexType::DataVev_& data,
    const typename IndexType::DataVev_& lookups, uint64_t pred_gran) {
  uint64_t size = lookups.size();
  typedef typename IndexType::K_ K;
  ResultInfo<K> res_info;
  for (uint64_t i = 0; i < size; i++) {
    SearchRange range;
    res_info.index_predict_time +=
        GetNsTime([&] { range = index.Lookup(lookups[i].first); });
    GetItemRange(&range, pred_gran, data.size());

    res_info.total_search_range += range.stop - range.start;
    if (range.stop - range.start > res_info.max_search_range) {
      res_info.max_search_range = range.stop - range.start;
    }

#if LAST_MILE_SEARCH == 0
    auto it = std::lower_bound(
        data.begin() + range.start, data.begin() + range.stop, lookups[i].first,
        [](const auto& lhs, const K key) { return lhs.first < key; });
#else
    auto it = data.begin() + range.start;
    while (it != data.begin() + range.stop) {
      if (it->first == lookups[i].first) {
        break;
      } else {
        it++;
      }
    }
#endif
    res_info.res += it->first;
    size_t cnt = 1;

    while (++it != data.end() && it->first == lookups[i].first &&
           cnt++ < MAX_NUM_QUALIFYING) {
      res_info.res += it->first;
    }
  }
  res_info.ops = size;
  return res_info;
}

#endif  // EXPERIMENTS_UTIL_H_
