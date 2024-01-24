#include <fstream>
#include <map>
#include <set>
#include <sstream>

#include "key_type.h"
#include "ycsb_utils/util_lid.h"

int main(int argc, char* argv[]) {
  char* endptr;
  if (argc != 6) {
    for (auto i = 0; i < argc; i++) {
      std::cout << i << ": " << argv[i] << std::endl;
    }
    std::cout << " Usage: " << argv[0] << std::endl
              << "  1. dataset_path" << std::endl
              << "  2. store_path" << std::endl
              << "  3. ycsb_path" << std::endl
              << "  4. #init_key" << std::endl
              << "  5. #query" << std::endl;
    return -1;
  }

  const std::string kDataPath = argv[1];
  const std::string kStorePath = argv[2];
  const std::string kYCSBPath = argv[3];
  const uint64_t kInitNum = strtoul(argv[4], &endptr, 10) * 1e6;
  const uint64_t kOpsNum = strtoul(argv[5], &endptr, 10) * 1e6;

  KeyVec keys = LoadKeys<Key>(argv[1]);
  if (!is_sorted(keys.begin(), keys.end())) {
    std::sort(keys.begin(), keys.end());
  }
  auto seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::shuffle(keys.begin(), keys.end(), std::default_random_engine(seed));

  DataVec init_data(kInitNum);
  for (uint64_t i = 0; i < kInitNum; i++) {
    init_data[i].first = keys[i];
  }
  std::sort(init_data.begin(), init_data.end(),
            [](auto const& a, auto const& b) { return a.first < b.first; });
  for (uint64_t i = 0; i < kInitNum; i++) {
    init_data[i].second = Value(i);
  }
  for (uint64_t i = kInitNum - 10; i < kInitNum; i++) {
    if (init_data[i].first == UINT64_MAX) {
      init_data[i].first = init_data[i - 1].first + 1;
    }
  }
  StoreData<Record>(init_data, kStorePath + INIT_SUFFIX);

  std::ifstream infile_txn(kYCSBPath);
  std::string insert("INSERT");
  std::string read("READ");
  std::string update("UPDATE");
  std::string scan("SCAN");
  std::string op;
  Key key;
  int range;

  std::vector<int> ops;

  int count = 0;
  KeyVec ops_data;
  std::vector<int> ranges;
  while ((count < kOpsNum) && infile_txn.good()) {
    infile_txn >> op >> key;
    if (op.compare(insert) == 0) {
      ops.push_back(OpsType::INSERT);

    } else if (op.compare(read) == 0) {
      ops.push_back(OpsType::READ);

    } else if (op.compare(update) == 0) {
      ops.push_back(OpsType::UPDATE);

    } else if (op.compare(scan) == 0) {
      infile_txn >> range;
      ops.push_back(OpsType::SCAN);
      ranges.push_back(range);

    } else {
      std::cout << "UNRECOGNIZED CMD!\n";
      break;
    }
    if (key > init_data.size()) {
      ops_data.push_back(keys[key]);
    } else {
      ops_data.push_back(init_data[key].first);
    }
    count++;
  }

  StoreData<int>(ops, kStorePath + OPS_SUFFIX);
  StoreData<Key>(ops_data, kStorePath + OPS_KEY_SUFFIX);
  if (ranges.size() > 0) {
    StoreData<int>(ranges, kStorePath + RANGE_LEN_SUFFIX);
  }
}
