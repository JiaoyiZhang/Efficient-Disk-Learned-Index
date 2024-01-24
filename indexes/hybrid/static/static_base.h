#ifndef INDEXES_HYBRID_STATIC_STATIC_INDEX_H_
#define INDEXES_HYBRID_STATIC_STATIC_INDEX_H_
#include <algorithm>
#include <iostream>
#include <vector>

#include "../../../ycsb_utils/structures.h"
#include "../../../ycsb_utils/util_search.h"

template <typename K, typename V>
class StaticIndex {
 public:
  StaticIndex() {}

  typedef K K_;
  typedef V V_;
  typedef std::pair<K_, V_> Record_;
  typedef std::vector<Record_> DataVec_;

  struct param_t {
    std::string filename;
    uint64_t page_bytes;
  };

  StaticIndex(param_t p) {
    data_file_ = p.filename;
    record_per_page_ = p.page_bytes / sizeof(Record_);
    data_number_ = 0;
    fd = DirectIOOpen(p.filename);
  }
  ~StaticIndex() { DirectIOClose(fd); }

  inline ResultInfo<K, V> LowerBound(const SearchRange& range, const K key,
                                     uint64_t length) {
#ifdef CHECK_CORRECTION
    FetchRange fetch_range =
        GetFetchRange(range, record_per_page_, page_number_);
    size_t s = fetch_range.pid_start * record_per_page_;
    size_t e = std::min((fetch_range.pid_end + 1) * record_per_page_ - 1,
                        data_.size() - 1);
    auto it = std::lower_bound(
        data_.begin() + s, data_.begin() + e + 1, key,
        [](const auto& lhs, const K& key) { return lhs.first < key; });
    if (data_[s].first > key || data_[e].first < key || it->first != key) {
      std::cout << "the range given by the static index is wrong!\tlookup:"
                << key << ",\tdata_[s].first:" << data_[s].first
                << ",\tdata_[e].first:" << data_[e].first << std::endl;
      std::cout << "s:" << s << ",\te:" << e << std::endl;
      std::cout << "it->first:" << it->first << ",\tit->second:" << it->second
                << std::endl;
      std::cout << "range.start:" << range.start << ",\tstop:" << range.stop
                << std::endl;
    }
#endif
    int last_id = record_per_page_;
    if ((range.stop - 1) / record_per_page_ == page_number_ - 1) {
      last_id = last_page_id_;
    }
    return NormalCoreLookup<K_, V_>(fd, range, key, kWorstCase,
                                    record_per_page_, length, page_number_,
                                    read_buf_, last_id);
  }

  inline void MergeData(DataVec_& dy_data, DataVec_& merged_data) {
#ifdef BREAKDOWN
    auto start = std::chrono::high_resolution_clock::now();
#endif
    merged_data = DataVec_(dy_data.size() + size());
#ifdef BREAKDOWN
    auto end = std::chrono::high_resolution_clock::now();
    if (merge_cnt >= 0) {
      init_lat +=
          std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
              .count();
    }
#endif
    if (size() > 0) {
#ifdef BREAKDOWN
      start = std::chrono::high_resolution_clock::now();
#endif
      GetDataVector(merged_data);
#ifdef BREAKDOWN
      end = std::chrono::high_resolution_clock::now();
      if (merge_cnt >= 0) {
        get_static_data_lat +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                .count();
      }
#endif
    }

#ifdef BREAKDOWN
    start = std::chrono::high_resolution_clock::now();
#endif
    int cnt = merged_data.size() - 1, i = dy_data.size() - 1, j = size() - 1;
    while (i >= 0 && j >= 0) {
      if (dy_data[i].first < merged_data[j].first) {
        merged_data[cnt--] = merged_data[j--];
      } else {
        merged_data[cnt--] = dy_data[i--];
      }
    }

    while (i >= 0) {
      merged_data[cnt--] = dy_data[i--];
    }

    data_number_ = merged_data.size();
    page_number_ = std::ceil(data_number_ * 1.0 / record_per_page_);
    last_page_id_ =
        record_per_page_ - (page_number_ * record_per_page_ - data_number_);

#ifdef BREAKDOWN
    end = std::chrono::high_resolution_clock::now();
    if (merge_cnt >= 0) {
      split_data_lat +=
          std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
              .count();
    }
    start = std::chrono::high_resolution_clock::now();
#endif
    DirectIOWrite(fd, merged_data, record_per_page_ * sizeof(Record_),
                  page_number_, read_buf_);
#ifdef BREAKDOWN
    end = std::chrono::high_resolution_clock::now();
    if (merge_cnt >= 0) {
      store_disk_lat +=
          std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
              .count();
    }
    merge_cnt++;
#endif

#ifdef CHECK_CORRECTION
    DataVec_ stored;
    GetDataVector(stored);
    for (size_t i = 0; i < stored.size(); i++) {
      if (stored[i].first != stored_data[i].first) {
        std::cout << "store " << i
                  << " wrong!\tstored[i].first:" << stored[i].first
                  << ",\tstored_data[i].first:" << stored_data[i].first
                  << std::endl;
      }
    }
    data_ = stored_data;
#endif
  }

  virtual void Build(DataVec_& new_data) = 0;

  inline V FindData(const SearchRange& range, const K_ key) {
    ResultInfo<K_, V_> res = LowerBound(range, key, 1);
    return res.val;
  }

  inline bool UpdateData(const SearchRange& range, const K_ key,
                         const V_ value) {
    ResultInfo<K_, V_> res = LowerBound(range, key, 1);
    return Update1Page(res.fd, res.pid, res.idx, key, value,
                       record_per_page_ * sizeof(Record_), read_buf_);
  }

  inline V ScanData(const SearchRange& range, const K key, const int length) {
    ResultInfo<K, V> res = LowerBound(range, key, length);
    return res.val;
  }

#ifdef BREAKDOWN
  inline void Breakdown() {
    if (merge_cnt > 0) {
      std::cout << "init_lat:" << init_lat / merge_cnt / 1e6 << " ms"
                << std::endl;
      std::cout << "get_static_data_lat:"
                << get_static_data_lat / merge_cnt / 1e6 << " ms" << std::endl;
      std::cout << "split_data_lat:" << split_data_lat / merge_cnt / 1e6
                << " ms" << std::endl;
      std::cout << "store_disk_lat:" << store_disk_lat / merge_cnt / 1e6
                << " ms" << std::endl;
    }
  }
#endif

  inline size_t size() const { return data_number_; }

  virtual size_t GetStaticInitSize(DataVec_& data) const = 0;

  virtual size_t GetNodeSize() const = 0;
  virtual size_t GetTotalSize() const = 0;

  virtual std::string GetIndexName() const { return name_; }

 private:
  inline void GetDataVector(DataVec_& data) const {
    if (size() > 0) {
      GetAllData<K, V>(fd, 0, page_number_, record_per_page_, data_number_,
                       read_buf_, data);
    } else {
      std::cout << "GetDataVector: no data" << std::endl;
    }
  }

 private:
  std::string name_ = "DISK_STATIC_BASE";
#ifdef CHECK_CORRECTION
  DataVec_ data_;
#endif

 protected:
  std::string data_file_;
  int fd;
  uint64_t record_per_page_;

#ifdef BREAKDOWN
  double init_lat = 0.0;
  double get_static_data_lat = 0.0;
  double split_data_lat = 0.0;
  double store_disk_lat = 0.0;
  int merge_cnt = -1;
#endif

  uint64_t data_number_;
  int page_number_;
  int last_page_id_;
};

#endif