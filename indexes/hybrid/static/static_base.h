#ifndef INDEXES_HYBRID_STATIC_STATIC_INDEX_H_
#define INDEXES_HYBRID_STATIC_STATIC_INDEX_H_
#include <algorithm>
#include <iostream>
#include <vector>

#include "../../../utils/structures.h"
#include "../../../utils/util_search.h"

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
    if (range.stop / record_per_page_ == page_number_ - 1) {
      last_id = last_page_id_;
    }
    return NormalCoreLookup<K_, V_>(fd, range, key, kWorstCase,
                                    record_per_page_, length, page_number_,
                                    read_buf_, last_id);
  }

  inline void MergeData(DataVec_& dy_data, DataVec_& merged_data) {
    merged_data = DataVec_(dy_data.size() + size());
    DataVec_ stored_data = DataVec_(dy_data.size() + size());
    DataVec_ static_data;
    if (size() > 0) {
      GetDataVector(static_data);
    }
    uint64_t cnt = 0, i = 0, j = 0;
    for (; i < dy_data.size() && j < static_data.size();) {
      if (dy_data[i].first < static_data[j].first) {
        if (cnt > 0 && dy_data[i].first == merged_data[cnt - 1].first) {
          cnt--;
        }
        stored_data[cnt] = dy_data[i];
        merged_data[cnt] = {dy_data[i].first, cnt};
        i++;
      } else if (dy_data[i].first > static_data[j].first) {
        if (cnt > 0 && static_data[j].first == merged_data[cnt - 1].first) {
          cnt--;
        }
        stored_data[cnt] = static_data[j];
        merged_data[cnt] = {static_data[j].first, cnt};
        j++;
      } else {
        // ensure unique keys
        stored_data[cnt] = dy_data[i];
        merged_data[cnt] = {dy_data[i].first, cnt};
        i++;
        j++;
      }
      cnt++;
    }

    if (i < dy_data.size()) {
      for (; i < dy_data.size(); i++) {
        stored_data[cnt] = dy_data[i];
        merged_data[cnt] = {dy_data[i].first, cnt};
        cnt++;
      }
    }
    if (j < static_data.size()) {
      for (; j < static_data.size(); j++) {
        stored_data[cnt] = static_data[j];
        merged_data[cnt] = {static_data[j].first, cnt};
        cnt++;
      }
    }

    merged_data.resize(cnt);
    stored_data.resize(cnt);
    data_number_ = merged_data.size();
    page_number_ = std::ceil(data_number_ * 1.0 / record_per_page_);
    last_page_id_ =
        record_per_page_ - (page_number_ * record_per_page_ - data_number_);
    DirectIOWrite(fd, stored_data, record_per_page_ * sizeof(Record_),
                  page_number_, read_buf_);
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

  inline size_t size() const { return data_number_; }

  virtual size_t GetStaticInitSize(DataVec_& data) const = 0;

  virtual size_t GetNodeSize() const = 0;
  virtual size_t GetTotalSize() const = 0;

  virtual std::string GetIndexName() const { return name_; }

 private:
  inline void GetDataVector(DataVec_& data) const {
    if (size() > 0) {
      data = GetAllData<K, V>(fd, page_number_, record_per_page_, data_number_,
                              read_buf_);
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

  uint64_t data_number_;
  int page_number_;
  int last_page_id_;
};

#endif