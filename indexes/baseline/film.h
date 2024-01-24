#ifndef INDEXES_FILM_H_
#define INDEXES_FILM_H_

#include "../base_index.h"
#include "../film/film.h"

template <typename K, typename V>
class BaselineFILM : public BaseIndex<K, V> {
 public:
  struct param_t {
    int error_bound;
    size_t init_data_size;
    size_t page_size;
    size_t memory_threshold;
    double insert_fraction;
    std::string main_file;
  };

  typedef filminsert::FILMinsert<K, K *> filmadatype;

  typedef pair<typename filmadatype::Leafpiece *, unsigned short int>
      filmadalrupair;  // lru 中value 的type ，是一个pair，first is
                       // 所属的leaf，second 是slot in leaf
  typedef adalru::hashLRU<
      K, typename filmadatype::Leafpiece *,
      typename adalru::Node<K, typename filmadatype::Leafpiece *> *>
      filmadalrutype;
  typedef adalru::localLRU<unsigned short, K *> locallrutype;
  typedef filmstorage::filmdisk<K> filmadadisk;
  typedef filmstorage::filmmemory<K, K *, filmadatype, filmadalrutype,
                                  filmadadisk>
      filmadamemory;
  typedef typename filmadamemory::memoryusage memoryusage;

  typedef filmstorage::filmmemory<K, K *, filmadatype, filmadalrutype,
                                  filmadadisk>
      MemoryType;

  BaselineFILM(param_t params) : index_file_(params.main_file), p_(params) {
    filmada_ = filmadatype(0, params.error_bound, params.error_bound);
    interchain_ = new filmadalrutype(params.init_data_size * 1.2);
    std::cout << "interchain:" << interchain_ << std::endl;
    memoryfilm_ = MemoryType(params.init_data_size, params.memory_threshold,
                             &filmada_, interchain_);

    diskfilm_ = filmstorage::filmdisk<K>(
        params.main_file, params.page_size / 8,
        params.page_size / 8 / (sizeof(K) + sizeof(V)), 2);
    filmada_.valuesize = 1;       // ?
    memoryfilm_.reserveMem = 10;  // ?
    periodV_ = memoryfilm_.reserveMem * 1024 * 1024 /
               ((filmada_.valuesize + 1) * sizeof(K) * 10);
    fstream fs;
    fs.open(params.main_file.c_str(), ios::in);
    if (fs) {
      fs.close();
      remove(params.main_file.c_str());
    }
    int fd = -1;
    int ret = -1;
    uint64_t file_size = 2 * params.init_data_size * (sizeof(K) + sizeof(V));
    fd = open(params.main_file.c_str(), O_CREAT | O_RDWR, 0666);
    if (fd < 0) {
      printf("fd < 0");
      return;
    }

    ret = posix_fallocate(fd, 0, file_size);
    if (ret < 0) {
      printf("ret = %d, errno = %d,  %s\n", ret, errno, strerror(errno));
      return;
    }

    printf("fallocate create %.2fG file\n", file_size / 1024 / 1024 / 1024.0);
    close(fd);
  }

  void Build(typename BaseIndex<K, V>::DataVec_ &key_value) {
    query_stats_.qtype = "query";
    query_stats_.insert_frac = p_.insert_fraction;
    query_stats_.out_of_order_frac = p_.insert_fraction;
    V payload[filmada_.valuesize];
    for (auto i = 0; i < filmada_.valuesize; i++) {
      payload[i] = i;
    }
    std::vector<K> init_keys(key_value.size());
    for (int i = 0; i < key_value.size(); i++) {
      init_keys[i] = key_value[i].first;
    }
    std::cout << "build 0 over!" << std::endl;
    memoryfilm_.append(init_keys, payload, p_.error_bound, p_.error_bound);
    std::cout << "build 1 over!" << std::endl;

    pair<bool, filminsert::memoryusage> transflag = memoryfilm_.judgetransfer();
    unsigned int transleaves;
    unsigned int leaves = transflag.second.meminfo["leaves"];
    if (memoryfilm_.inpage == NULL) {
      memoryfilm_.createinmempage(
          diskfilm_.pagesize,
          diskfilm_.recordnum);  // create a page in memory;
    }
    std::cout << "build 2 over!" << std::endl;
    // evict according to lru
    int cnt = 0;
    while (transflag.first) {
      memoryfilm_.filmtransfer(transflag.second.meminfo["totalusage"],
                               &diskfilm_);
      transflag = memoryfilm_.judgetransfer();
      cnt++;
    }
    std::cout << "Build FILM over! index_file:" << index_file_
              << ",\tmethod film_ada_lru,\t available_memory "
              << p_.memory_threshold + 10 << ",\terror: " << p_.error_bound
              << ",\tpagesize " << (p_.page_size / 8 * 8 / 1024)
              << "k,\t datasize: " << p_.init_data_size << std::endl;

    std::cout << "init_state: film,\tinit_write_time "
              << diskfilm_.initwtime / 1000000.0 << ",\tpages_init_write "
              << diskfilm_.nextpageid << std::endl;
    for (map<string, double>::iterator iter = transflag.second.meminfo.begin();
         iter != transflag.second.meminfo.end(); iter++) {
      std::cout << iter->first << " " << iter->second << std::endl;
    }
  }

  V Find(const K key) {
    std::pair<K, K *> res;
    auto index_res = filmada_.search_one(key, &query_stats_);
    if (index_res.find == false) {
      // read data from the sort_piece
      if (index_res.flags) {
        query_stats_.memnum += 1;
        auto finddata = (adalru::Node<lruOff_type, K *> *)
                            filmada_.sort_list->slotdata[index_res.slot];
        res.first = key;
        res.second = finddata->value;
        // update intrachain
        filmada_.sort_list->intrachain.moveTohead(finddata);
        // update interchain_
        memoryfilm_.lru->put(filmada_.sort_list->slotkey[0],
                             filmada_.sort_list);
      } else {
        query_stats_.disknum += 1;
        query_stats_.diskpagenum += 1;
        auto writeevict =
            (std::pair<pageid_type, pageOff_type> *)index_res.findleaf
                ->slotdata[index_res.slot];  // {page id, offset}
        res = diskfilm_.odirectreadfromdisk(writeevict);

        index_res.findleaf->slotdata[index_res.slot] =
            index_res.findleaf->intrachain.put(index_res.slot, res.second);
        index_res.findleaf->slotflag[index_res.slot] = true;

        memoryfilm_.lru->put(index_res.findleaf->startkey,
                             index_res.findleaf);  // update interchain
        memoryfilm_.evictPoss.emplace_back(writeevict);
        filmada_.inkeynum++;
        filmada_.exkeynum--;
      }
    } else {
      // read data from memory
      if (index_res.flags) {
        query_stats_.memnum += 1;
        auto finddata = (adalru::Node<lruOff_type, K *> *)
                            index_res.findleaf->slotdata[index_res.slot];
        res.first = key;
        res.second = finddata->value;
        // update intrachain
        index_res.findleaf->intrachain.moveTohead(finddata);
        // update interchain
        memoryfilm_.lru->put(index_res.findleaf->startkey, index_res.findleaf);
      } else {
        // read data from the disk
        query_stats_.disknum += 1;
        query_stats_.diskpagenum += 1;
        auto writeevict = (pair<pageid_type, pageOff_type> *)
                              index_res.findleaf->slotdata[index_res.slot];

        if (writeevict->first == memoryfilm_.inpage->pageid) {
          res.second = new K[filmada_.valuesize];
          K *inmemdata = memoryfilm_.inpage->inmemdata;
          res.first = inmemdata[writeevict->second];
          for (int ki = 0; ki < filmada_.valuesize; ki++) {
            res.second[ki] = inmemdata[writeevict->second + 1 + ki];
          }
        } else {
          res = diskfilm_.odirectreadfromdisk(writeevict);
        }

        index_res.findleaf->slotdata[index_res.slot] =
            index_res.findleaf->intrachain.put(index_res.slot, res.second);
        index_res.findleaf->slotflag[index_res.slot] = true;
        // update interchain
        memoryfilm_.lru->put(index_res.findleaf->startkey, index_res.findleaf);

        memoryfilm_.evictPoss.emplace_back(writeevict);
        filmada_.inkeynum++;
        filmada_.exkeynum--;
      }
    }

    return res.second[0];
  }

  V Scan(const K key, const int range) {
    // TODO
    return 0;
  }

  bool Insert(const K key, const V value) {
    V payload[filmada_.valuesize]{value};
    filmada_.update_random(key, payload, interchain_);
    // }
    filmada_.root = &filmada_.innerlevels.back()->innerpieces[0];
    auto a = filmada_.leaflevel.opt->get_segment(filmada_.m_tailleaf->endkey);
    filmada_.m_tailleaf->update(a);

    // judge whether to evict data, after doing inserts
    auto transflag = memoryfilm_.judgetransfer(&query_stats_);
    query_stats_.computetimeuse = 0.0;
    while (transflag.first) {
      runtimeevictkeytopage2(&memoryfilm_, transflag.second.totalusemem,
                             &filmada_, &diskfilm_, &query_stats_);
      transflag = memoryfilm_.judgetransfer(&query_stats_);
    }
    return true;
  }

  bool Update(const K key, const V value) {
    // TODO
    return true;
  }

  bool Delete(const K key) {
    // TODO
    return true;
  }

  size_t GetNodeSize() { return 0; }

  size_t GetTotalSize() { return GetNodeSize() + 0; }

  void PrintEachPartSize() {
    pair<bool, filminsert::memoryusage> transflag = memoryfilm_.judgetransfer();
    for (map<string, double>::iterator iter = transflag.second.meminfo.begin();
         iter != transflag.second.meminfo.end(); iter++) {
      std::cout << iter->first << " " << iter->second << std::endl;
    }
    query_stats_.print_stats();
  }

  std::string GetIndexName() const { return name_; }

  param_t GetIndexParams() const { return param_t(0); }

 private:
  std::string name_ = "BASELINE_FILM";
  std::string index_file_;

  MemoryType memoryfilm_;
  filmadatype filmada_;
  filmadalrutype *interchain_;
  filmstorage::filmdisk<K> diskfilm_;

  filminsert::access_stats query_stats_;  // ?
  int periodV_;                           // ?
  unsigned int num_out_inserts = 160000 * 2;
  size_t retrain_updates = 0;

  param_t p_;
};

#endif  // !INDEXES_FILM_H_