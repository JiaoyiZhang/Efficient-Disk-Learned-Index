#ifndef INDEXES_HYBRID_STATIC_MULTI_THREADED_STATIC_INDEX_H_
#define INDEXES_HYBRID_STATIC_MULTI_THREADED_STATIC_INDEX_H_
#include <algorithm>
#include <atomic>
#include <iostream>
#include <vector>

#include "../../../ycsb_utils/structures.h"
#include "../../../ycsb_utils/util_search.h"

template <typename K, typename V>
class MultiThreadedStaticIndex {
 public:
  MultiThreadedStaticIndex() {}

  typedef K K_;
  typedef V V_;
  typedef std::pair<K_, V_> Record_;
  typedef std::vector<Record_> DataVec_;

  struct param_t {
    std::string filename;
    uint64_t page_bytes;
    uint64_t thread_numbers;
    uint64_t merge_thread_numbers;
  };

  class PartitionRange {
   public:
    PartitionRange() {
      thread_id = -1;
      start_page_id_ = 0;
      static_page_num_ = 0;
      static_data_num_ = 0;
      dynamic_start_idx_ = 0;
      dynamic_end_idx_ = 0;
      stored_start_pid_ = 0;
    }
    void SetRange(uint64_t start_pid, uint64_t page_num, uint64_t data_num,
                  int64_t dy_start, uint64_t dy_end,
                  uint64_t stored_start_pid) {
      thread_id = -1;
      start_page_id_ = start_pid;
      static_page_num_ = page_num;
      static_data_num_ = data_num;
      dynamic_start_idx_ = dy_start;
      dynamic_end_idx_ = dy_end;
      stored_start_pid_ = stored_start_pid;
    }

    void SetThreadID(int tid) { thread_id = tid; }

   public:
    int thread_id;
    uint64_t start_page_id_;
    uint64_t static_page_num_;
    uint64_t static_data_num_;
    uint64_t dynamic_start_idx_;
    uint64_t dynamic_end_idx_;
    uint64_t stored_start_pid_;
  };

  class ThreadParams {
   public:
    ThreadParams() {
      fd_ = -1;
      version_ = 0;
    }
    ~ThreadParams() {
      if (fd_ != -1) {
        DirectIOClose(fd_);
      }
    }
    void FreeBuffer() { free(buf_); }

    void PrepareBuffer(uint64_t page_bytes) {
      buf_ = reinterpret_cast<K_*>(
          aligned_alloc(page_bytes, page_bytes * ALLOCATED_BUF_SIZE));
    }
    void UpdateFile(int new_fd, uint64_t ver) {
      if (fd_ != -1) {
        DirectIOClose(fd_);
      }
      fd_ = new_fd;
      version_ = ver;
    }
    inline std::pair<int*, uint64_t*> GetInfo() { return {&fd_, &version_}; }
    inline void SetFD(int fd) { fd_ = fd; }
    inline void SetVersion(int ver) { version_ = ver; }

    inline int GetFD() const { return fd_; }
    inline int GetVersion() const { return version_; }

   public:
    K_* buf_ = NULL;  // read/write buffer

   private:
    int fd_;            // file descriptor
    uint64_t version_;  // file version
  };

  MultiThreadedStaticIndex(param_t p)
      : data_file_(p.filename),
        record_per_page_(p.page_bytes / sizeof(Record_)),
        thread_numbers_(p.thread_numbers),
        threads_(std::vector<ThreadParams>(p.thread_numbers)),
        merge_thread_num_(p.merge_thread_numbers),
        partition_keys_(
            std::vector<K_>(merge_thread_num_, std::numeric_limits<K>::max())),
        partitions_(std::vector<PartitionRange>(merge_thread_num_)),
        data_numbers_(std::vector<uint64_t>(p.merge_thread_numbers, 0)),
        page_start_ids_(std::vector<uint64_t>(p.merge_thread_numbers, 0)),
        page_last_ids_(std::vector<uint64_t>(p.merge_thread_numbers, 0)) {
    consistent_version_cnt_.store(thread_numbers_);
    old_version_ = 1;

    std::string filename =
        data_file_ + std::to_string(latest_version_.load() + 1);
    for (uint64_t i = 0; i < thread_numbers_; i++) {
      threads_[i].PrepareBuffer(p.page_bytes);
    }
#ifdef CHECK_CORRECTION
    partition_min_keys_ =
        std::vector<K_>(merge_thread_num_, std::numeric_limits<K>::max());
#endif
#ifdef BREAKDOWN
    partition_lat = std::vector<double>(thread_numbers_, 0);
    get_dynamic_range_lat = std::vector<double>(thread_numbers_, 0);
    obtain_task_lat = std::vector<double>(thread_numbers_, 0);
    try_to_obtain_task_cnt = std::vector<double>(thread_numbers_, 0);
    init_vector_lat = std::vector<double>(thread_numbers_, 0);
    get_static_data_lat = std::vector<double>(thread_numbers_, 0);
    merge_sorted_array_lat = std::vector<double>(thread_numbers_, 0);
    open_file_lat = std::vector<double>(thread_numbers_, 0);
    store_disk_lat = std::vector<double>(thread_numbers_, 0);
    store_page_num = std::vector<double>(thread_numbers_, 0);
    model_training_lat = std::vector<double>(thread_numbers_, 0);
    merge_cnt = 0;
#endif
  }

  inline void Init(DataVec_& init_data, int thread_id) {
    std::string filename = data_file_ + std::to_string(1);
    auto fd = DirectIOOpen(filename);
    threads_[thread_id].UpdateFile(fd, 1);
    int sub_items = std::ceil(init_data.size() * 1.0 / merge_thread_num_);
    for (int i = 0, prev_page_cnt = 0; i < merge_thread_num_; i++) {
      int last_item_idx =
          std::min((i + 1) * sub_items, static_cast<int>(init_data.size())) - 1;
      DataVec_ sub_data(init_data.begin() + i * sub_items,
                        init_data.begin() + last_item_idx + 1);

      partition_keys_[i] = init_data[last_item_idx].first;
#ifdef CHECK_CORRECTION
      partition_min_keys_[i] = init_data[i * sub_items].first;
#endif
      data_numbers_[i] = sub_data.size();
      page_start_ids_[i] = prev_page_cnt;
      page_last_ids_[i] =
          page_start_ids_[i] + (sub_data.size() - 1) / record_per_page_;
      auto curr_pages = page_last_ids_[i] - page_start_ids_[i] + 1;
      prev_page_cnt += curr_pages;
      size_t page_byte = record_per_page_ * sizeof(Record_);
      DirectIOWrite(fd, sub_data, page_byte, curr_pages,
                    threads_[thread_id].buf_, page_start_ids_[i] * page_byte);
#ifdef PRINT_MULTI_THREAD_INFO
      std::cout << "partition " << i << ",\tlast idx:" << last_item_idx
                << ",\tpartition min key:" << partition_min_keys_[i]
                << ",\tpartition key:" << partition_keys_[i]
                << ",\tdata_numbers_:" << data_numbers_[i]
                << ",\tpage_start_ids_:" << page_start_ids_[i]
                << ",\tpage_last_ids_:" << page_last_ids_[i] << std::endl;
#endif
    }
    latest_version_.store(1);
    consistent_version_cnt_.store(1);

    for (size_t i = 0; i < thread_numbers_; i++) {
      if (i != thread_id) {
        auto fd = DirectIOOpen(filename);
        threads_[i].UpdateFile(fd, latest_version_.load());
        consistent_version_cnt_.fetch_add(1);
      }
    }

#ifdef CHECK_CORRECTION
    int cnt = 0;
    for (size_t p = 0; p < merge_thread_num_; p++) {
      DataVec_ stored(data_numbers_[p]);
      GetSubData(stored, thread_id, page_start_ids_[p],
                 page_last_ids_[p] - page_start_ids_[p] + 1, data_numbers_[p]);
      for (size_t i = 0; i < stored.size(); i++, cnt++) {
        if (stored[i].first != init_data[cnt].first) {
          std::cout << "Init store " << i
                    << " wrong!\tstored[i].first:" << stored[i].first
                    << ",\tinit_data[cnt].first:" << init_data[cnt].first
                    << std::endl;
        }
      }
    }
    data_ = std::vector<DataVec_>(merge_thread_num_);
    for (int i = 0; i < merge_thread_num_; i++) {
      int s = i * sub_items;
      int e = std::min((i + 1) * sub_items, static_cast<int>(init_data.size()));
      data_[i].insert(data_[i].begin(), init_data.begin() + s,
                      init_data.begin() + e);
    }
#endif
  }

  // only copy the current data info
  MultiThreadedStaticIndex& operator=(const MultiThreadedStaticIndex& other) {
    name_ = other.name_;
    data_file_ = other.data_file_;
    record_per_page_ = other.record_per_page_;

    thread_numbers_ = other.thread_numbers_;
    uint64_t ver = other.latest_version_.load();
    latest_version_.store(ver);
    consistent_version_cnt_.store(thread_numbers_);
    for (uint64_t i = 0; i < thread_numbers_; i++) {
      std::string filename =
          data_file_ + std::to_string(latest_version_.load());
      int fd = DirectIOOpen(filename);
      threads_[i].SetFD(fd);
      threads_[i].SetVersion(other.threads_[i].GetVersion());
      threads_[i].buf_ = other.threads_[i].buf_;
    }
    old_version_ = other.old_version_;
    merge_thread_num_ = other.merge_thread_num_;
    partition_keys_ = other.partition_keys_;
#ifdef CHECK_CORRECTION
    partition_min_keys_ = other.partition_min_keys_;
#endif
    partitions_ = other.partitions_;

    data_numbers_ = other.data_numbers_;
    page_start_ids_ = other.page_start_ids_;
    page_last_ids_ = other.page_last_ids_;

    processing_thread_num_.store(-1);
    finished_thread_num_.store(0);
    return *this;
  }

  inline void DeleteFile() {
    std::string filename = data_file_ + std::to_string(latest_version_.load());
#ifdef PRINT_MULTI_THREAD_INFO
    std::cout << "deleting " << filename << std::endl;
#endif
    std::remove(filename.c_str());
  }

  inline void FreeBuffer() {
    for (size_t i = 0; i < thread_numbers_; i++) {
      threads_[i].FreeBuffer();
    }
  }

  inline ResultInfo<K, V> LowerBound(const SearchRange& range, const K key,
                                     uint64_t length, int thread_id,
                                     int partition_id) {
#ifdef CHECK_CORRECTION
    int page_cnt =
        page_last_ids_[partition_id] - page_start_ids_[partition_id] + 1;
    FetchRange fetch_range = GetFetchRange(range, record_per_page_, page_cnt);
    size_t s = fetch_range.pid_start * record_per_page_;
    size_t e = std::min((fetch_range.pid_end + 1) * record_per_page_ - 1,
                        data_[partition_id].size() - 1);
    auto it = std::lower_bound(
        data_[partition_id].begin() + s, data_[partition_id].begin() + e + 1,
        key, [](const auto& lhs, const K& key) { return lhs.first < key; });
    if (data_[partition_id][s].first > key ||
        data_[partition_id][e].first < key || it->first != key) {
      std::cout << "the range given by the static index is wrong!\tlookup:"
                << key << ",\tdata_[s].first:" << data_[partition_id][s].first
                << ",\tdata_[e].first:" << data_[partition_id][e].first
                << std::endl;
      std::cout << "s:" << s << ",\te:" << e << std::endl;
      std::cout << "it->first:" << it->first << ",\tit->second:" << it->second
                << std::endl;
      std::cout << "range.start:" << range.start << ",\tstop:" << range.stop
                << std::endl;
    }
#endif
    int last_id = record_per_page_;
    if ((range.stop - 1) / record_per_page_ + page_start_ids_[partition_id] ==
        page_last_ids_[partition_id]) {
      last_id = data_numbers_[partition_id] -
                record_per_page_ * (page_last_ids_[partition_id] -
                                    page_start_ids_[partition_id]);
    }
    return NormalCoreLookup<K_, V_>(
        threads_[thread_id].GetFD(), range, key, kWorstCase, record_per_page_,
        length, page_last_ids_[partition_id], threads_[thread_id].buf_, last_id,
        page_start_ids_[partition_id]);
  }

  inline void MergeSubData(DataVec_& dy_data, DataVec_& merged_data,
                           int thread_id, int partition_id) {
#ifdef BREAKDOWN
    auto start = std::chrono::high_resolution_clock::now();
#endif
    auto static_data_num = partitions_[partition_id].static_data_num_;
#ifdef PRINT_MULTI_THREAD_INFO
    std::cout << "in staticbase::MergeData, size():" << size() << ",\tthread "
              << thread_id << ",\tpartition id:" << partition_id
              << ",\tstart_pid:" << partitions_[partition_id].start_page_id_
              << ",\tstatic_page_num:"
              << partitions_[partition_id].static_page_num_
              << ",\tstatic_data_num:" << static_data_num << std::endl;
#endif
    auto dy_size = partitions_[partition_id].dynamic_end_idx_ -
                   partitions_[partition_id].dynamic_start_idx_;
    merged_data = DataVec_(dy_size + static_data_num);
#ifdef BREAKDOWN
    auto end = std::chrono::high_resolution_clock::now();
    init_vector_lat[thread_id] +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
#endif
    if (partitions_[partition_id].static_data_num_ > 0) {
#ifdef BREAKDOWN
      start = std::chrono::high_resolution_clock::now();
#endif
      GetSubData(merged_data, thread_id,
                 partitions_[partition_id].start_page_id_,
                 partitions_[partition_id].static_page_num_, static_data_num);
#ifdef BREAKDOWN
      end = std::chrono::high_resolution_clock::now();
      get_static_data_lat[thread_id] +=
          std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
              .count();
#endif
    }
#ifdef PRINT_MULTI_THREAD_INFO
    std::cout << ",\tthread " << thread_id << ",\tpartition id:" << partition_id
              << ",\tpartitions_[partition_id].start_page_id_:"
              << partitions_[partition_id].start_page_id_ << std::endl;

    std::cout << ",\tthread " << thread_id << ",\tpartition id:" << partition_id
              << ",\tfirst_dy_idx:"
              << partitions_[partition_id].dynamic_start_idx_
              << ",\tend_dy_idx:" << partitions_[partition_id].dynamic_end_idx_
              << std::endl;
#endif
#ifdef BREAKDOWN
    start = std::chrono::high_resolution_clock::now();
#endif
    MergeTwoSortedArray(dy_data, partitions_[partition_id].dynamic_start_idx_,
                        partitions_[partition_id].dynamic_end_idx_, merged_data,
                        static_data_num);
#ifdef BREAKDOWN
    end = std::chrono::high_resolution_clock::now();
    merge_sorted_array_lat[thread_id] +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
#endif
    data_numbers_[partition_id] = merged_data.size();
    page_start_ids_[partition_id] = partitions_[partition_id].stored_start_pid_;
    page_last_ids_[partition_id] = page_start_ids_[partition_id] +
                                   (merged_data.size() - 1) / record_per_page_;
    int merged_page_num =
        page_last_ids_[partition_id] - page_start_ids_[partition_id] + 1;

#ifdef PRINT_MULTI_THREAD_INFO
    std::cout << "thread " << thread_id << ",\tpid:" << partition_id
              << ",\tstatic data:" << static_data_num
              << ",\tmerged_data.size():" << merged_data.size()
              << ",\tmerged_page_start_idx:" << page_start_ids_[partition_id]
              << ",\tmerged_page_last_idx:" << page_last_ids_[partition_id]
              << ",\tmerged_page_num:" << merged_page_num << std::endl;
    std::cout << "thread " << thread_id << ",\tpid:" << partition_id
              << ",\tmin key:" << merged_data[0].first
              << ",\tmax key:" << merged_data[merged_data.size() - 1].first
              << std::endl;
#endif
#ifdef BREAKDOWN
    start = std::chrono::high_resolution_clock::now();
#endif
    uint64_t old_ver = latest_version_.load();
    std::string filename = data_file_ + std::to_string(old_ver + 1);
    int fd = DirectIOOpen(filename);
    threads_[thread_id].UpdateFile(fd, old_ver + 1);
    size_t page_byte = record_per_page_ * sizeof(Record_);
#ifdef BREAKDOWN
    end = std::chrono::high_resolution_clock::now();
    open_file_lat[thread_id] +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
    start = std::chrono::high_resolution_clock::now();
#endif
    DirectIOWrite(fd, merged_data, page_byte, merged_page_num,
                  threads_[thread_id].buf_,
                  page_start_ids_[partition_id] * page_byte);
#ifdef BREAKDOWN
    end = std::chrono::high_resolution_clock::now();
    store_disk_lat[thread_id] +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
    store_page_num[thread_id] += merged_page_num;
#endif

    // change for model training
    for (size_t i = 0; i < merged_data.size(); i++) {
      merged_data[i].second = i;
    }
    partition_keys_[partition_id] = merged_data[merged_data.size() - 1].first;
#ifdef PRINT_MULTI_THREAD_INFO
    std::cout << "in thread " << thread_id << ", curr fd:" << fd << std::endl;
    std::cout << "in thread " << thread_id << ",\tpid:" << partition_id
              << ",\tpartition key:" << partition_keys_[partition_id]
              << std::endl;
#endif

#ifdef CHECK_CORRECTION
    partition_min_keys_[partition_id] = merged_data[0].first;
    DataVec_ check_res(data_numbers_[partition_id]);
    GetSubData(check_res, thread_id, page_start_ids_[partition_id],
               merged_page_num, data_numbers_[partition_id]);
    for (size_t i = 1; i < check_res.size(); i++) {
      if (check_res[i].first < check_res[i - 1].first) {
        std::cout << "partition " << partition_id << " store " << i
                  << " wrong!\tstored[i].first:" << check_res[i].first
                  << ",\tstored[i-1].first:" << check_res[i - 1].first
                  << std::endl;
      }
      if (check_res[i].first != merged_data[i].first) {
        std::cout << "partition " << partition_id << " store " << i
                  << " wrong!\tstored[i].first:" << check_res[i].first
                  << ",\tmerged_data[i].first:" << merged_data[i].first
                  << std::endl;
      }
    }
    data_[partition_id] = check_res;
#endif

    // Compare And Swap
    consistent_version_cnt_.fetch_add(1);
#ifdef PRINT_MULTI_THREAD_INFO
    std::cout << "in thread " << thread_id
              << ", new consistent_version_cnt_:" << consistent_version_cnt_
              << std::endl;
    std::cout << "latest_version_:" << latest_version_ << std::endl;
    std::cout << "now thread id:" << thread_id << std::endl;
    for (uint64_t i = 0; i < thread_numbers_; i++) {
      std::cout << " ------- " << thread_id << " ------- [" << i
                << "]: fd:" << threads_[i].GetFD()
                << ",\tver:" << threads_[i].GetVersion() << std::endl;
    }
#endif
  }

  inline void FinishMerge(int thread_id) {
#ifdef PRINT_MULTI_THREAD_INFO
    std::cout << "tid:" << thread_id
              << ",\tnow #finished:" << finished_thread_num_ << std::endl;
#endif
    finished_thread_num_.fetch_add(1);
#ifdef PRINT_MULTI_THREAD_INFO
    std::cout << "in thread " << thread_id
              << ", new finished_thread_num_:" << finished_thread_num_
              << std::endl;
#endif
  }

  virtual void Build(DataVec_& new_data, int thread_id) = 0;

  inline std::pair<int*, uint64_t*> GetCurrThreadInfo(int thread_id) {
    return threads_[thread_id].GetInfo();
  }

  inline bool AllSubMergeFinished() {
    return finished_thread_num_.load() == merge_thread_num_;
  }

  inline bool GetUpdatedStatus() {
    return old_version_ != latest_version_.load();
  }

  inline bool UpdateLatestVersion(int thread_id) {
    bool updated = false;
    bool success = updated_.compare_exchange_strong(updated, true);
    if (success) {
      uint64_t old_ver = latest_version_.load();
#ifdef PRINT_MULTI_THREAD_INFO
      std::cout << "latest_version_:" << latest_version_ << std::endl;
#endif
      latest_version_.compare_exchange_strong(old_ver, old_ver + 1);

      finished_thread_num_.store(0);
#ifdef PRINT_MULTI_THREAD_INFO
      std::cout << "thread " << thread_id
                << " updated latest_version_:" << latest_version_ << std::endl;
      std::cout << "AllSubMergeFinished! tid:" << thread_id
                << ",\tfinished_thread_num_:" << finished_thread_num_.load()
                << ",\tmerge_thread_num_:" << merge_thread_num_ << std::endl;
#endif
    }
    return success;
  }

  inline bool CheckLatestVersion(int thread_id) {
    if (!GetUpdatedStatus()) {
      return false;
    }
    if (threads_[thread_id].GetVersion() <
        static_cast<int>(latest_version_.load())) {
#ifdef PRINT_MULTI_THREAD_INFO
      std::cout << "thread " << thread_id << " update version, old fd"
                << threads_[thread_id].GetFD()
                << ",\tver:" << threads_[thread_id].GetVersion()
                << ",\tlatest_version_.load():" << latest_version_.load()
                << ",\tcpr:"
                << (threads_[thread_id].GetVersion() ==
                    static_cast<int>(latest_version_.load()))
                << (threads_[thread_id].GetVersion() == latest_version_.load())
                << std::endl;
#endif
      // the version of this thread is outdated, update fd
      std::string filename =
          data_file_ + std::to_string(latest_version_.load());
      auto fd = DirectIOOpen(filename);
      threads_[thread_id].UpdateFile(fd, latest_version_);
      consistent_version_cnt_.fetch_add(1);
#ifdef PRINT_MULTI_THREAD_INFO
      std::cout << "thread " << thread_id << " update version, new fd"
                << threads_[thread_id].GetFD()
                << ",\tver:" << threads_[thread_id].GetVersion() << std::endl;
      std::cout << "consistent_version_cnt_ " << consistent_version_cnt_
                << std::endl;
#endif
#ifdef CHECK_CORRECTION
      if (consistent_version_cnt_ > thread_numbers_) {
        std::cout << "tid:" << thread_id
                  << ",\tconsistent_version_cnt_:" << consistent_version_cnt_
                  << ",\tthread_numbers_:" << thread_numbers_ << std::endl;
      }
#endif
    }

    return consistent_version_cnt_.load() == thread_numbers_;
  }

  inline int ObtainMergeTask(int thread_id) {
#ifdef BREAKDOWN
    auto start = std::chrono::high_resolution_clock::now();
    try_to_obtain_task_cnt[thread_id]++;
#endif
    int partition_id = processing_thread_num_.load();
    if (partition_id >= 0 &&
        partition_id < static_cast<int>(merge_thread_num_) &&
        (threads_[thread_id].GetVersion() == latest_version_.load())) {
#ifdef PRINT_MULTI_THREAD_INFO
      std::cout << "thread" << thread_id
                << ",\tver:" << threads_[thread_id].GetVersion()
                << ",\tobtain partition " << partition_id
                << " latest_version_:" << latest_version_.load() << std::endl;
#endif
      // obtain a task
      auto success = processing_thread_num_.compare_exchange_strong(
          partition_id, partition_id + 1);
      if (success) {
        partitions_[partition_id].thread_id = thread_id;
#ifdef BREAKDOWN
        auto end = std::chrono::high_resolution_clock::now();
        obtain_task_lat[thread_id] +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                .count();
#endif
        return partition_id;
      }
    }
#ifdef BREAKDOWN
    auto end = std::chrono::high_resolution_clock::now();
    obtain_task_lat[thread_id] +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
#endif
    // no available task
    return -1;
  }

  inline void PartitionData(DataVec_& dy_data, int thread_id) {
#ifdef BREAKDOWN
    merge_cnt++;
    auto start = std::chrono::high_resolution_clock::now();
#endif
    updated_.store(false);
    old_version_ = latest_version_.load();
    for (size_t i = 0; i < merge_thread_num_; i++) {
      auto dy_range = GetDynamicRange(dy_data, i, thread_id);

      size_t stored_pid_start = 0;
      size_t last_page_cnt = 0;
      if (i > 0) {
        last_page_cnt = std::ceil((partitions_[i - 1].static_data_num_ +
                                   partitions_[i - 1].dynamic_end_idx_ -
                                   partitions_[i - 1].dynamic_start_idx_) *
                                  1.0 / record_per_page_);
        stored_pid_start = partitions_[i - 1].stored_start_pid_ + last_page_cnt;
      }

      partitions_[i].SetRange(
          page_start_ids_[i], page_last_ids_[i] - page_start_ids_[i] + 1,
          data_numbers_[i], dy_range.first, dy_range.second, stored_pid_start);

#ifdef PRINT_MULTI_THREAD_INFO
      std::cout << "----- partition " << i
                << ", start_page_id_:" << partitions_[i].start_page_id_
                << ",\tsta_page_num_:" << partitions_[i].static_page_num_
                << ",\tsta_data_num_:" << partitions_[i].static_data_num_
                << ",\tdy_start_idx_:" << partitions_[i].dynamic_start_idx_
                << ",\tdy_end_idx_:" << partitions_[i].dynamic_end_idx_
                << ",\tstored_pid_start:" << partitions_[i].stored_start_pid_
                << ",\tprev_page_cnt:" << last_page_cnt << std::endl;
#endif
    }
    processing_thread_num_.store(0);

    int t_num = thread_numbers_;
#ifdef PRINT_MULTI_THREAD_INFO
    std::cout << ", consistent_version_cnt_:" << consistent_version_cnt_
              << std::endl;
#endif
    consistent_version_cnt_.compare_exchange_strong(t_num, 0);
#ifdef CHECK_CORRECTION
    data_ = std::vector<DataVec_>(merge_thread_num_);
#endif
#ifdef BREAKDOWN
    auto end = std::chrono::high_resolution_clock::now();
    partition_lat[thread_id] +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
#endif
  }

  inline int GetPartitionID(const K_ key) {
    auto it =
        std::lower_bound(partition_keys_.begin(), partition_keys_.end(), key);
    return it - partition_keys_.begin();
  }

  inline V FindData(const SearchRange& range, const K_ key, int thread_id,
                    int partition_id) {
    ResultInfo<K_, V_> res = LowerBound(range, key, 1, thread_id, partition_id);
    return res.val;
  }

  inline bool UpdateData(const SearchRange& range, const K_ key, const V_ value,
                         int thread_id, int partition_id) {
    ResultInfo<K_, V_> res = LowerBound(range, key, 1, thread_id, partition_id);
    return Update1Page(res.fd, res.pid, res.idx, key, value,
                       record_per_page_ * sizeof(Record_),
                       threads_[thread_id].buf_);
  }

  inline V ScanData(const SearchRange& range, const K key, const int length,
                    int thread_id, int partition_id) {
    ResultInfo<K, V> res =
        LowerBound(range, key, length, thread_id, partition_id);
    return res.val;
  }

  inline size_t GetDiskBytes() const { return sizeof(Record_) * size(); }

  inline size_t size() const {
    size_t cnt = 0;
    for (size_t i = 0; i < merge_thread_num_; i++) {
      cnt += data_numbers_[i];
    }
    return cnt;
  }

#ifdef BREAKDOWN
  void PrintBreakdown() {
    std::cout << "***********BREAKDOWN***************" << std::endl;
    std::cout << "thread "
              << ",\tpartition lat/ms"
              << ",\tget_dy_range_lat/ms"
              << ",\tobtain_task_lat/ms"
              << ",\ttry to obtain"
              << ",\tinit_vector_lat/ms"
              << ",\tget_sta_data_lat/ms"
              << ",\tmerge_sorted_lat/ms"
              << ",\topen_file_lat/ms"
              << ",\tstore_disk_lat/ms"
              << ",\tstore_page_num"
              << ",\tmodel_training_lat" << std::endl;
    std::cout << "merge cnt:" << merge_cnt << std::endl;
    for (size_t i = 0; i < thread_numbers_; i++) {
      std::cout << i << ",\t" << partition_lat[i] / merge_cnt / 1e6 << ",\t"
                << get_dynamic_range_lat[i] / merge_cnt / 1e6 << ",\t"
                << obtain_task_lat[i] / merge_cnt / 1e6 << ",\t"
                << try_to_obtain_task_cnt[i] / merge_cnt << ",\t"
                << init_vector_lat[i] / merge_cnt / 1e6 << ",\t"
                << get_static_data_lat[i] / merge_cnt / 1e6 << ",\t"
                << merge_sorted_array_lat[i] / merge_cnt / 1e6 << ",\t"
                << open_file_lat[i] / merge_cnt / 1e6 << ",\t"
                << store_disk_lat[i] / merge_cnt / 1e6 << ",\t"
                << store_page_num[i] / merge_cnt << ",\t"
                << model_training_lat[i] / merge_cnt / 1e6 << std::endl;
    }
  }
#endif

  virtual size_t GetStaticInitSize(DataVec_& data) const = 0;

  virtual size_t GetNodeSize() const = 0;
  virtual size_t GetTotalSize() const = 0;

  virtual std::string GetIndexName() const { return name_; }

 private:
  inline std::pair<size_t, size_t> GetDynamicRange(DataVec_& dy_data,
                                                   size_t idx, int thread_id) {
#ifdef BREAKDOWN
    auto start = std::chrono::high_resolution_clock::now();
#endif
    size_t first_dy_idx = 0, end_dy_idx = 0;
    if (idx > 0) {
      auto it_l = std::lower_bound(
          dy_data.begin(), dy_data.end(), partition_keys_[idx - 1],
          [](const auto& lhs, const K& key) { return lhs.first < key; });
      first_dy_idx = it_l - dy_data.begin();
    }
    auto it_r = std::lower_bound(
        dy_data.begin(), dy_data.end(), partition_keys_[idx],
        [](const auto& lhs, const K& key) { return lhs.first < key; });
    end_dy_idx = it_r - dy_data.begin();
#ifdef BREAKDOWN
    auto end = std::chrono::high_resolution_clock::now();
    get_dynamic_range_lat[thread_id] +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
#endif
    return {first_dy_idx, end_dy_idx};
  }

  inline void GetSubData(DataVec_& data, int thread_id, int start_page_id,
                         int page_num, int length) {
#ifdef PRINT_MULTI_THREAD_INFO
    std::cout << "GetSubData, thread " << thread_id
              << ",\tfd:" << threads_[thread_id].GetFD()
              << ",\tver:" << threads_[thread_id].GetVersion()
              << ",\tstart_page_id:" << start_page_id
              << ",\tpage_num:" << page_num << std::endl;
#endif
    GetAllData<K, V>(threads_[thread_id].GetFD(), start_page_id, page_num,
                     record_per_page_, length, threads_[thread_id].buf_, data);
#ifdef CHECK_CORRECTION
    for (int i = 1; i < length; i++) {
      if (data[i].first <= data[i - 1].first) {
        std::cout << "in GetSubData.2 store " << i
                  << " wrong!\tstored[i].first:" << data[i].first
                  << ",\tstored[i-1].first:" << data[i - 1].first << std::endl;
      }
    }
#endif
  }

  inline void MergeTwoSortedArray(DataVec_& dy_data, int first_dy_idx,
                                  int end_dy_idx, DataVec_& merged_data,
                                  int static_size) {
    int cnt = end_dy_idx - first_dy_idx + static_size - 1, i = end_dy_idx - 1,
        j = static_size - 1;
    int tmp = 0;
    while (i >= first_dy_idx && j >= 0) {
      if (dy_data[i].first > merged_data[j].first) {
        merged_data[cnt--] = dy_data[i--];
      } else {
        merged_data[cnt--] = merged_data[j--];
      }
      tmp++;
    }

    while (i >= first_dy_idx) {
      merged_data[cnt--] = dy_data[i--];
    }

#ifdef CHECK_CORRECTION
    for (i = 1; i < merged_data.size(); i++) {
      if (merged_data[i].first < merged_data[i - 1].first) {
        std::cout << " store " << i
                  << " wrong!\tstored[i].first:" << merged_data[i].first
                  << ",\tstored[i-1].first:" << merged_data[i - 1].first
                  << std::endl;
      }
    }
#endif
  }

 private:
  std::string name_ = "DISK_STATIC_BASE";
#ifdef CHECK_CORRECTION
  std::vector<DataVec_> data_;
#endif

 protected:
  std::string data_file_;
  uint64_t record_per_page_;

  uint64_t thread_numbers_;
  std::atomic<uint64_t> latest_version_{0b000};
  std::atomic<int> consistent_version_cnt_{-1};
  uint64_t old_version_;
  std::atomic<bool> updated_{false};
  std::vector<ThreadParams> threads_;

  uint64_t merge_thread_num_;
  std::vector<K_> partition_keys_;
#ifdef CHECK_CORRECTION
  std::vector<K_> partition_min_keys_;
#endif
  std::vector<PartitionRange> partitions_;

  std::vector<uint64_t> data_numbers_;
  std::vector<uint64_t> page_start_ids_;
  std::vector<uint64_t> page_last_ids_;

  std::atomic<int> processing_thread_num_{-1};
  std::atomic<uint64_t> finished_thread_num_{0};

#ifdef BREAKDOWN
  std::vector<double> partition_lat;
  std::vector<double> get_dynamic_range_lat;
  std::vector<double> obtain_task_lat;
  std::vector<double> try_to_obtain_task_cnt;
  std::vector<double> init_vector_lat;
  std::vector<double> get_static_data_lat;
  std::vector<double> merge_sorted_array_lat;
  std::vector<double> open_file_lat;
  std::vector<double> store_disk_lat;
  std::vector<double> store_page_num;
  std::vector<double> model_training_lat;
  int merge_cnt = -1;
#endif
};

#endif