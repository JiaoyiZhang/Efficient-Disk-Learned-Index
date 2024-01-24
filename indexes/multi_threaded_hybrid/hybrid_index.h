#ifndef INDEXES_MULTI_THREADED_HYBRID_INDEX_H_
#define INDEXES_MULTI_THREADED_HYBRID_INDEX_H_

#include <assert.h>

#include <atomic>
#include <thread>

#include "../base_index.h"

#define INIT_SIZE 100
enum ModeType { NormalMode, PrepareToMerge, MergingMode, MergedMode };

template <typename K, typename V, typename DynamicType, typename StaticType>
class MultiThreadedHybridIndex : public MultiThreadedBaseIndex<K, V> {
 public:
  struct param_t {
    typename DynamicType::param_t d_params_;
    typename StaticType::param_t s_params_;
    size_t merge_ratio_;
  };

  MultiThreadedHybridIndex(param_t params)
      : index_params_(params),
        merge_cnt_(0),
        mem_find_cnt_(0),
        disk_find_cnt_(0),
        mem_update_cnt_(0),
        disk_update_cnt_(0),
        mem_insert_cnt_(0),
        max_dynamic_usage_(0),
        max_dynamic_index_usage_(0),
        max_memory_usage_(0),
        max_buffer_size_(0),
        merge_ratio_(params.merge_ratio_) {
    merging_dynamic_index_ = NULL;
    dynamic_index_ = new DynamicType(params.d_params_);
    static_index_ = new StaticType(params.s_params_);
    backup_static_index_ = NULL;
    mode_.store(NormalMode);
    threads_free_status_ = std::vector<std::atomic<int>>(
        params.s_params_.disk_params.thread_numbers);
    for (size_t i = 0; i < threads_free_status_.size(); i++) {
      threads_free_status_[i].store(0);
    }
#ifdef BREAKDOWN
    assign_to_merge_lat = std::vector<double>(threads_free_status_.size(), 0);
    update_version_lat = std::vector<double>(threads_free_status_.size(), 0);
    wait_to_normal_lat = std::vector<double>(threads_free_status_.size(), 0);
    update_all_version_lat =
        std::vector<double>(threads_free_status_.size(), 0);
    merge_lat = std::vector<double>(threads_free_status_.size(), 0);
    merge_cnt = std::vector<double>(threads_free_status_.size(), 0);
    merged_wait_lat = std::vector<double>(threads_free_status_.size(), 0);
    merging_wait_lat = std::vector<double>(threads_free_status_.size(), 0);
    prepare_wait_lat = std::vector<double>(threads_free_status_.size(), 0);
    normal_wait_lat = std::vector<double>(threads_free_status_.size(), 0);
#endif
  }

  typedef typename MultiThreadedBaseIndex<K, V>::DataVec_ BaseVec;

  void Build(BaseVec& data) {
    // collect some data for learned indexes to train models
    size_t init_size = INIT_SIZE;
    BaseVec dynamic_data(init_size);
    BaseVec static_data(data.size());
    assert(static_data.size() > init_size);
    uint64_t seg = static_data.size() / init_size;
    uint64_t dynamic_cnt = 0, static_cnt = 0, total_cnt = 0;

    auto it = data.begin();
    while (it != data.end()) {
      if (total_cnt % seg == 0 && dynamic_cnt < init_size) {
        dynamic_data[dynamic_cnt++] = {it->first, it->second};
      } else {
        static_data[static_cnt++] = {it->first, it->second};
      }
      it++;
      total_cnt++;
    }
    dynamic_data.resize(dynamic_cnt);
    static_data.resize(static_cnt);
#ifdef PRINT_MULTI_THREAD_INFO
    std::cout << "#init_records in dynamic_index_:" << dynamic_data.size()
              << std::endl;
    std::cout << "#init_records in static_index_:" << static_data.size()
              << std::endl;
#endif

    DynamicType* dy = dynamic_index_.load();
    dy->Build(dynamic_data);

    StaticType* sta = static_index_.load();
    sta->Build(static_data, 0);

    // get the remaining memory budget for the dynamic index
    size_t static_memory = sta->GetNodeSize();
#ifdef PRINT_MULTI_THREAD_INFO
    std::cout << "merge ratio:" << merge_ratio_
              << " ,\tstatic_memory:" << PRINT_MIB(static_memory) << " MiB"
              << std::endl;
#endif
    if (static_memory * merge_ratio_ >= data.size() * sizeof(std::pair<K, V>)) {
      throw std::runtime_error("Need smaller merge ratio!");
    }

    max_memory_usage_ = GetCurrMemoryUsage();
    max_dynamic_usage_ = dy->GetTotalSize();
    max_dynamic_index_usage_ = dy->GetNodeSize();
#ifdef CHECK_CORRECTION
    for (size_t i = 0; i < dynamic_cnt; i++) {
      auto res = dy->Find(dynamic_data[i].first);
      if (dynamic_data[i].second != res) {
        std::cout << "find dynamic " << i << ",\tk:" << dynamic_data[i].first
                  << ",\tv:" << dynamic_data[i].second << ",\tres:" << res
                  << std::endl;
      }
    }
    std::cout << "Check dynamic index over! " << std::endl;
#endif
  }

  V Find(const K key, int thread_id) {
    threads_free_status_[thread_id].fetch_add(1);
    V res = std::numeric_limits<V>::max();
    switch (mode_.load()) {
      case NormalMode:
        break;
      case PrepareToMerge:
        break;
      case MergingMode:
        AssignedToMerge(thread_id);
        break;
      case MergedMode:
        UpdateVersion(thread_id);
        break;
      default:
        break;
    }

    // lookup in the dynamic index
    DynamicType* dy = dynamic_index_.load();
    res = dy->Find(key);
    mem_find_cnt_++;

    // lookup in the merging index when the mode is merging/merged mode
    if (res == std::numeric_limits<V>::max()) {
      switch (mode_.load()) {
        case NormalMode:
          break;
        case PrepareToMerge:
          break;
        case MergingMode: {
          DynamicType* merge_dy = merging_dynamic_index_.load();
          if (merge_dy != NULL) {
            ready_to_normal_.store(false);
            ops_on_merge_dynamic_.fetch_add(1);
            res = merge_dy->Find(key);
            UpdateReadyNormalStatus();
          }
          break;
        }
        case MergedMode: {
          if (ready_to_normal_.load()) {
            break;
          }
          ready_to_normal_.store(false);
          ops_on_merge_dynamic_.fetch_add(1);
          DynamicType* merge_dy = merging_dynamic_index_.load();
          res = merge_dy->Find(key);
          UpdateReadyNormalStatus();
          break;
        }
        default:
          break;
      }
    }

    // lookup on static index (on disk)
    if (res == std::numeric_limits<V>::max()) {
      switch (mode_.load()) {
          // normal and merging modes lookup in the static index
        case NormalMode:
        case PrepareToMerge:
        case MergingMode: {
          StaticType* sta = static_index_.load();
          res = sta->Find(key, thread_id);
          break;
        }
          // merged mode lookup in the backup static index
        case MergedMode: {
          auto backup_static = backup_static_index_.load();
          bool find_in_static = true;
          if (backup_static && backup_static->GetUpdatedStatus()) {
            UpdateVersion(thread_id);
            find_in_static = false;
          }
          backup_static = backup_static_index_.load();
          if (backup_static == NULL || find_in_static) {
            StaticType* sta = static_index_.load();
            res = sta->Find(key, thread_id);
          } else {
            res = backup_static->Find(key, thread_id);
          }
          break;
        }
        default:
          break;
      }
      disk_find_cnt_++;
    }
    threads_free_status_[thread_id].fetch_sub(1);
    return res;
  }

  V Scan(const K key, const int range, int thread_id) {
    // TODO: update the content
    DynamicType* dy = dynamic_index_.load();
    V res = dy->Scan(key, range);
    mem_find_cnt_++;
    disk_find_cnt_++;
    UpdateVersion(thread_id);
    StaticType* sta = static_index_.load();
    if (res == std::numeric_limits<V>::max()) {
      res = sta->Scan(key, range, thread_id);
    } else {
      res += sta->Scan(key, range, thread_id);
    }
    return res;
  }

  bool Insert(const K key, const V value, int thread_id) {
    threads_free_status_[thread_id].fetch_add(1);
    bool res = false;
    DynamicType* dy = dynamic_index_.load();
    switch (mode_.load()) {
      case NormalMode: {
        size_t curr_disk = GetTotalSize();
        size_t curr_memory = GetCurrMemoryUsage();
        if ((curr_disk - curr_memory) * 1.0 / curr_memory <= merge_ratio_) {
          ModeType mode = NormalMode;
          if (!mode_.compare_exchange_strong(mode, PrepareToMerge)) {
            break;
          }

#ifdef PRINT_MULTI_THREAD_INFO
          std::cout << "insert key " << key << " wait for merge!" << std::endl;
#endif
#ifdef BREAKDOWN
          auto start = std::chrono::high_resolution_clock::now();
#endif
          int cnt = 0;
          while (ready_to_merge_.load() == false) {
            auto timeout = yield(cnt++);
            if (timeout) {
              break;
            }
          }
#ifdef BREAKDOWN
          auto end = std::chrono::high_resolution_clock::now();
          normal_wait_lat[thread_id] +=
              std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                  .count();
#endif

#ifdef PRINT_MULTI_THREAD_INFO
          StaticType* sta = static_index_.load();
          auto static_size = sta->size();
          std::cout << "insert key " << key
                    << " need to merge! dynamic_size:" << dy->size()
                    << ",\tstatic_size:" << static_size
                    << ",\tmax_buffer_size_:" << max_buffer_size_
                    << ",\ttotal size:" << PRINT_MIB(curr_disk)
                    << ",\tmemory usage:" << PRINT_MIB(curr_memory)
                    << std::endl;
#endif
          mode = PrepareToMerge;

          if (mode_.compare_exchange_strong(mode, MergingMode)) {
            dy = dynamic_index_.load();
            max_memory_usage_ = std::max(max_memory_usage_, curr_memory);
            max_dynamic_usage_ =
                std::max(max_dynamic_usage_, dy->GetTotalSize());
            max_dynamic_index_usage_ =
                std::max(max_dynamic_index_usage_, dy->GetNodeSize());
            max_buffer_size_ = std::max(max_buffer_size_, dy->size());
            DynamicType* dynamic_null = NULL;
            merging_dynamic_index_.compare_exchange_strong(
                dynamic_null, dynamic_index_.load());
            dy = new DynamicType(index_params_.d_params_);
            dynamic_index_.store(dy);
#ifdef PRINT_MULTI_THREAD_INFO
            std::cout << "thread " << thread_id << ",\t call merge!"
                      << std::endl;
#endif
            merge_cnt_++;
            Merge(thread_id);
          }
#ifdef PRINT_MULTI_THREAD_INFO
          else {
            std::cout << "the dynamic index is under merging " << std::endl;
          }
#endif
        }
        break;
      }
      case PrepareToMerge: {
#ifdef BREAKDOWN
        auto start = std::chrono::high_resolution_clock::now();
#endif
        int cnt = 0;
        while (mode_.load() == PrepareToMerge) {
          auto timeout = yield(cnt++);
          if (timeout) {
            break;
          }
        }
#ifdef BREAKDOWN
        auto end = std::chrono::high_resolution_clock::now();
        prepare_wait_lat[thread_id] +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                .count();
#endif
        break;
      }
      case MergingMode: {
        AssignedToMerge(thread_id);
        size_t curr_disk = GetTotalSize();
        size_t curr_memory = GetCurrMemoryUsage();
#ifdef BREAKDOWN
        auto start = std::chrono::high_resolution_clock::now();
#endif
        int cnt = 0;
        while (mode_.load() == MergingMode &&
               (curr_disk - curr_memory) * 1.0 / curr_memory <= merge_ratio_) {
          auto timeout = yield(cnt++);
          if (timeout) {
            break;
          }
          curr_disk = GetTotalSize();
          curr_memory = GetCurrMemoryUsage();
          AssignedToMerge(thread_id);
        }
#ifdef BREAKDOWN
        auto end = std::chrono::high_resolution_clock::now();
        merging_wait_lat[thread_id] +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                .count();
#endif
        break;
      }
      case MergedMode: {
        UpdateVersion(thread_id);
        size_t curr_disk = GetTotalSize();
        size_t curr_memory = GetCurrMemoryUsage();
#ifdef BREAKDOWN
        auto start = std::chrono::high_resolution_clock::now();
#endif
        int cnt = 0;
        while (mode_.load() == MergedMode &&
               (curr_disk - curr_memory) * 1.0 / curr_memory <= merge_ratio_) {
          auto timeout = yield(cnt++);
          if (timeout) {
            UpdateAllVersion();
            break;
          }
          curr_disk = GetTotalSize();
          curr_memory = GetCurrMemoryUsage();
          UpdateVersion(thread_id);
        }
#ifdef BREAKDOWN
        auto end = std::chrono::high_resolution_clock::now();
        merged_wait_lat[thread_id] +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                .count();
#endif
        break;
      }
      default: {
        break;
      }
    }
    mem_insert_cnt_++;

    // insert into dynamic index
    ready_to_merge_.store(false);
    ops_on_dynamic_.fetch_add(1);
    dy = dynamic_index_.load();
    if (mode_.load() == PrepareToMerge) {
      UpdateReadyMergeStatus();
      threads_free_status_[thread_id].fetch_sub(1);
      return Insert(key, value, thread_id);
    }
    res = dy->Insert(key, value);
    UpdateReadyMergeStatus();

#ifdef CHECK_CORRECTION
    if (ops_on_dynamic_ < 0) {
      std::cout << "tid:" << thread_id << ",\tops on dy:" << ops_on_dynamic_
                << std::endl;
    }
    V new_val = dy->Find(key);
    if (new_val != value) {
      std::cout << "insert wrong! key:" << key << ",\tval:" << value
                << ",\tnew_val:" << new_val << std::endl;
      dy->Find(key);
    }
#endif

    threads_free_status_[thread_id].fetch_sub(1);
    return res;
  }

  bool Update(const K key, const V value, int thread_id) {
    // TODO: update the code
    return true;
  }

  bool Delete(const K key, int thread_id) {
    // TODO: update the code
    return true;
  }

  inline size_t GetCurrMemoryUsage() const {
    DynamicType* dy = dynamic_index_.load();
    StaticType* sta = static_index_.load();
    return dy->GetTotalSize() + sta->GetNodeSize();
  }
  inline size_t GetNodeSize() const { return max_memory_usage_; }
  inline size_t GetTotalSize() const {
    DynamicType* dy = dynamic_index_.load();
    StaticType* sta = static_index_.load();
    return dy->GetTotalSize() + sta->GetTotalSize();
  }
  void PrintEachPartSize() {
    DynamicType* dy = dynamic_index_.load();
    StaticType* sta = static_index_.load();
    std::cout << "-------------dynamic info-------------" << std::endl;
    dy->PrintEachPartSize();
    std::cout << "-------------static info---------------" << std::endl;
    sta->PrintEachPartSize();
    std::cout << "-------------processing info-------------" << std::endl;
    std::cout << "\t\tmerge cnt:" << merge_cnt_
              << ",\tin-memory find cnt:" << mem_find_cnt_
              << ",\ton-disk find cnt:" << disk_find_cnt_
              << ",\tin-memory insert:" << mem_insert_cnt_ << std::endl;
    std::cout << "-------------memory usage---------------" << std::endl;
    std::cout << "\tmerge_ratio:" << merge_ratio_
              << ",\tmax_buffer_size:" << max_buffer_size_
              << ",\tmax_buffer_usage_:"
              << PRINT_MIB(max_buffer_size_ * sizeof(std::pair<K, V>)) << " MiB"
              << std::endl;
    std::cout << "\tmax_dynamic_usage_:" << PRINT_MIB(max_dynamic_usage_)
              << " MiB,\tmax_dynamic_index_usage_:"
              << PRINT_MIB(max_dynamic_index_usage_)
              << " MiB,\tmax_dynamic_data_node_usage:"
              << PRINT_MIB(max_dynamic_usage_ - max_dynamic_index_usage_)
              << " MiB,\tmax_static_usage_:" << PRINT_MIB(sta->GetNodeSize())
              << " MiB,\tmax_memory_usage_:" << PRINT_MIB(max_memory_usage_)
              << " MiB" << std::endl;
    std::cout << "-------------print over---------------" << std::endl;
  }
  std::string GetIndexName() const {
    return GetDynamicName() + "_" + GetStaticName();
  }
  typename DynamicType::param_t GetDynamicParams() const {
    DynamicType* dy = dynamic_index_.load();
    return dy->GetIndexParams();
  }
  typename StaticType::param_t GetStaticParams() const {
    StaticType* sta = static_index_.load();
    return sta->GetIndexParams();
  }
  size_t size() const {
    DynamicType* dy = dynamic_index_.load();
    size_t cnt = dy->size();
    DynamicType* merging_dy = merging_dynamic_index_.load();
    if (merging_dy != NULL) {
      cnt += merging_dy->size();
    }
    StaticType* sta = static_index_.load();
    cnt += sta->size();
    return cnt;
  }
  void FreeBuffer() {
    auto sta = static_index_.load();
    sta->FreeBuffer();
  }
#ifdef BREAKDOWN
  void PrintBreakdown() {
    std::cout << "***********HYBRID BREAKDOWN***************" << std::endl;
    std::cout << "thread id"
              << ",\tassign_to_merge_lat/ms"
              << ",\tupdate_version_lat/ms"
              << ",\twait_to_normal_lat/ms"
              << ",\tupdate_all_version_lat/ms"
              << ",\tmerge_lat/ms"
              << ",\tmerge_cnt_"
              << ",\tmerged_wait_lat/ms"
              << ",\tmerging_wait_lat/ms"
              << ",\tprepare_wait_lat/ms"
              << ",\tnormal_wait_lat/ms" << std::endl;
    for (int i = 0; i < assign_to_merge_lat.size(); i++) {
      std::cout << i << ",\t" << assign_to_merge_lat[i] / merge_cnt_ / 1e6
                << ",\t" << update_version_lat[i] / merge_cnt_ / 1e6 << ",\t"
                << wait_to_normal_lat[i] / merge_cnt_ / 1e6 << ",\t"
                << update_all_version_lat[i] / merge_cnt_ / 1e6 << ",\t"
                << merge_lat[i] / merge_cnt_ / 1e6 << ",\t" << merge_cnt[i]
                << ",\t" << merged_wait_lat[i] / merge_cnt_ / 1e6 << ",\t"
                << merging_wait_lat[i] / merge_cnt_ / 1e6 << ",\t"
                << prepare_wait_lat[i] / merge_cnt_ / 1e6 << ",\t"
                << normal_wait_lat[i] / merge_cnt_ / 1e6 << std::endl;
    }
    auto sta = static_index_.load();
    sta->PrintBreakdown();
  }
#endif

 private:
  void Merge(int thread_id) {
#ifdef BREAKDOWN
    auto start = std::chrono::high_resolution_clock::now();
#endif
    auto merging_dy = merging_dynamic_index_.load();

    // collect the data points in dynamic stage
    tmp_dynamic_data_ = BaseVec();
    merging_dy = merging_dynamic_index_.load();
    merging_dy->Merge(tmp_dynamic_data_);
    StaticType* backup_sta = new StaticType(index_params_.s_params_);
    // copy the current info into the backup static index
    *backup_sta = *(static_index_.load());
    backup_static_index_.store(backup_sta);
    backup_sta->Merge(tmp_dynamic_data_, thread_id);
#ifdef BREAKDOWN
    auto end = std::chrono::high_resolution_clock::now();
    merge_lat[thread_id] +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
    merge_cnt[thread_id]++;
#endif
  }

  inline void AssignedToMerge(int thread_id) {
    StaticType* backup_sta = backup_static_index_.load();
    if (backup_sta == NULL) {
      return;
    }
#ifdef BREAKDOWN
    auto start = std::chrono::high_resolution_clock::now();
#endif

    auto partition_id = backup_sta->ObtainMergeTask(thread_id);
    if (partition_id >= 0) {
#ifdef PRINT_MULTI_THREAD_INFO
      DynamicType* dy = dynamic_index_.load();
      std::cout << "thread" << thread_id << ",\tobtain partition "
                << partition_id
                << " AssignedToMerge.1 tmp_dynamic_data_.size():"
                << tmp_dynamic_data_.size() << ",\tdynamic size:" << dy->size()
                << std::endl;
#endif
      backup_sta->MergeSubData(tmp_dynamic_data_, thread_id, partition_id);
#ifdef PRINT_MULTI_THREAD_INFO
      std::cout << "thread" << thread_id
                << " AssignedToMerge.2 tmp_dynamic_data_.size():"
                << tmp_dynamic_data_.size() << ",\tdynamic size:" << dy->size()
                << std::endl;
#endif
    }
    if (mode_.load() == MergingMode && backup_sta->AllSubMergeFinished()) {
      ModeType now_mode = MergingMode;
      bool change_mode = mode_.compare_exchange_strong(now_mode, MergedMode);
      if (change_mode) {
        backup_sta->UpdateLatestVersion(thread_id);
#ifdef PRINT_MULTI_THREAD_INFO
        std::cout << "thread " << thread_id
                  << " AssignedToMerge.3 tmp_dynamic_data_.size():"
                  << tmp_dynamic_data_.size()
                  << ",\tdynamic size:" << dy->size() << std::endl;
        std::cout << "tid:" << thread_id << ",\tchange mode to:" << mode_.load()
                  << " success!" << std::endl;
#endif
      }
#ifdef PRINT_MULTI_THREAD_INFO
      else {
        std::cout << "tid:" << thread_id << ",\tchange mode to:" << mode_.load()
                  << " failed!" << std::endl;
      }
#endif
    }
#ifdef BREAKDOWN
    auto end = std::chrono::high_resolution_clock::now();
    assign_to_merge_lat[thread_id] +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
#endif
  }

  inline void UpdateVersion(int thread_id) {
#ifdef BREAKDOWN
    auto start = std::chrono::high_resolution_clock::now();
#endif
    auto backup_static = backup_static_index_.load();
    if (backup_static && mode_.load() == MergedMode &&
        backup_static->GetUpdatedStatus() &&
        backup_static->CheckLatestVersion(thread_id)) {
      // Compare And Swap
#ifdef PRINT_MULTI_THREAD_INFO
      std::cout << "thread " << thread_id
                << " try to update backup_static_index_!" << std::endl;
#endif
      auto now_static = static_index_.load();
      auto success = false;
      if (now_static != backup_static) {
        success =
            static_index_.compare_exchange_strong(now_static, backup_static);
      }
      auto succ2 =
          backup_static_index_.compare_exchange_strong(backup_static, NULL);
      if (!success) {
#ifdef PRINT_MULTI_THREAD_INFO
        std::cout << "thread " << thread_id
                  << " try to update backup_static_index_ failed" << std::endl;
#endif
#ifdef BREAKDOWN
        auto end = std::chrono::high_resolution_clock::now();
        update_version_lat[thread_id] +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                .count();
#endif
        return;
      }
#ifdef PRINT_MULTI_THREAD_INFO
      std::cout << "thread " << thread_id
                << " try to delete file:" << std::endl;
      std::cout << "res 1:" << success << ",\tres2:" << succ2 << std::endl;
      std::cout << "static_index_:" << static_index_
                << ",\tnow_static:" << now_static
                << ",\tbackup_static:" << backup_static
                << ",\tbackup_static_index_:" << backup_static_index_
                << std::endl;
#endif
#ifdef BREAKDOWN
      auto start0 = std::chrono::high_resolution_clock::now();
#endif
      now_static->DeleteFile();

      int spin_cnt = 0;
      while (ready_to_normal_.load() == false) {
        auto timeout = yield(spin_cnt++);
        if (timeout) {
          break;
        }
      }
      merging_dynamic_index_.store(NULL);
#ifdef PRINT_MULTI_THREAD_INFO
      std::cout << "thread " << thread_id
                << " try to update backup_static_index_ successed" << std::endl;
#endif
      ModeType now_mode = MergedMode;
      mode_.compare_exchange_strong(now_mode, NormalMode);
#ifdef BREAKDOWN
      auto end0 = std::chrono::high_resolution_clock::now();
      wait_to_normal_lat[thread_id] +=
          std::chrono::duration_cast<std::chrono::nanoseconds>(end0 - start0)
              .count();
#endif
    }
#ifdef BREAKDOWN
    auto end = std::chrono::high_resolution_clock::now();
    update_version_lat[thread_id] +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
#endif
  }
  inline void UpdateReadyNormalStatus() {
    auto prev = ops_on_merge_dynamic_.fetch_sub(1);
    if (prev == 1) {
      ready_to_normal_.store(true);
    }
  }

  inline void UpdateReadyMergeStatus() {
    auto prev = ops_on_dynamic_.fetch_sub(1);
    if (prev == 1) {
      ready_to_merge_.store(true);
    }
  }

  inline void UpdateAllVersion() {
    for (size_t i = 0; i < threads_free_status_.size(); i++) {
      if (threads_free_status_[i].load() == 0 && mode_.load() == MergedMode) {
        std::cout << "update all:" << i << std::endl;
        UpdateVersion(i);
      }
    }
  }

  bool yield(int count) {
    if (count > 100000000) {
      return true;
    } else if (count > 10) {
      std::this_thread::sleep_for(std::chrono::microseconds(1));
    } else if (count > 3) {
      sched_yield();
    } else {
      _mm_pause();
    }
    return false;
  }

  std::string GetDynamicName() const {
    DynamicType* dy = dynamic_index_.load();
    return dy->GetIndexName();
  }

  std::string GetStaticName() const {
    StaticType* sta = static_index_.load();
    return sta->GetIndexName();
  }

#ifdef BREAKDOWN
  std::vector<double> assign_to_merge_lat;
  std::vector<double> update_version_lat;
  std::vector<double> wait_to_normal_lat;
  std::vector<double> update_all_version_lat;
  std::vector<double> merge_lat;
  std::vector<double> merge_cnt;
  std::vector<double> merged_wait_lat;
  std::vector<double> merging_wait_lat;
  std::vector<double> prepare_wait_lat;
  std::vector<double> normal_wait_lat;
#endif
  std::atomic<DynamicType*> dynamic_index_;
  std::atomic<DynamicType*> merging_dynamic_index_;
  std::atomic<StaticType*> static_index_;
  std::atomic<StaticType*> backup_static_index_;
  std::atomic<ModeType> mode_;

  std::atomic<int> ops_on_dynamic_{0};
  std::atomic<int> ops_on_merge_dynamic_{0};
  std::atomic<bool> ready_to_merge_{true};
  std::atomic<bool> ready_to_normal_{true};
  std::vector<std::atomic<int>> threads_free_status_;

  BaseVec tmp_dynamic_data_;
  param_t index_params_;

  size_t merge_cnt_;
  size_t mem_find_cnt_;
  size_t disk_find_cnt_;
  size_t mem_update_cnt_;
  size_t disk_update_cnt_;
  size_t mem_insert_cnt_;

  size_t max_dynamic_usage_;
  size_t max_dynamic_index_usage_;
  size_t max_memory_usage_;
  size_t max_buffer_size_;

  size_t merge_ratio_;
};

#endif  // !INDEXES_MULTI_THREADED_HYBRID_INDEX_H_