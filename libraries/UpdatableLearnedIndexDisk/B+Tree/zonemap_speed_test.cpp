#include "mt_b_tree.h"
#include <cstring>
#include <iostream>
#include <thread>
#include <random>

const int num_leaf_nodes = 586e3;
const int num_level1_nodes = 2289;
const int num_inserts = 2e4;
void zone_map_test(int num_threads) {
  ThreadSafeStorageManager sm("test.bin");
  sm.direct_open("test.bin");
  std::vector<std::mutex> mutexes((num_leaf_nodes - 1) / (num_leaf_nodes / num_level1_nodes) + 1);
  std::vector<std::thread> threads;
  auto start = std::chrono::system_clock::now();
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back([&, i]() {
      auto seed = std::random_device()();
      std::mt19937 gen(seed);
      std::uniform_int_distribution<> dis(0, num_leaf_nodes - 1);
      const int num_actions = num_inserts / num_threads;
      void* data = sm.alloc();
      for (int i = 0; i < num_actions; i++) {
        auto pos = dis(gen);
        auto block = pos / (num_leaf_nodes / num_level1_nodes);
        // std::unique_lock<std::mutex> lock(mutexes[block]);
        sm.read_block(block, data);
        // sm.write_block(block, data);
      }
    });
  }
  for (auto &t : threads) {
    t.join();
  }
  auto end = std::chrono::system_clock::now();
  auto ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
  std::cout << "num_threads," << num_threads<<", insert time," << ns / 1e6 << "ns,speed," << (double)(num_inserts) / ns * 1e9
            << ",ops/s"<<std::endl;
}

int main() {
  std::cout<<"num_leaf_nodes,"<<num_leaf_nodes<<",num_level1_nodes,"<<num_level1_nodes<<",num_inserts,"<<num_inserts<<std::endl;
  zone_map_test(1);
  zone_map_test(2);
  zone_map_test(4);
  zone_map_test(8);
  zone_map_test(16);
  zone_map_test(32);
  return 0;
}
