#include "mt_b_tree.h"

#include <cstring>
#include <iostream>
#include <thread>

template <class T>
void seq_test(T &btree, std::size_t n) {
  for (std::size_t i = 0; i < n; i++) {
    btree.insert(i, i + 10000);
  }
  for (std::size_t j = 0; j < n; j++) {
    assert(btree.lookup(j) == j + 10000);
  }
}

template <class T>
void random_insert_test(T &btree, std::size_t n) {
  // random insert
  std::vector<int> v(n);
  for (std::size_t i = 0; i < n; i++) {
    v[i] = i;
  }
  std::random_shuffle(v.begin(), v.end());
  for (std::size_t i = 0; i < n; i++) {
    btree.insert(v[i], v[i] + 10000);
  }
  for (std::size_t j = 0; j < n; j++) {
    assert(btree.lookup(j) == j + 10000);
  }
  btree.is_valid();
}

void multi_thread_test(std::size_t n, std::size_t num_threads, std::size_t preload_n) {
  ThreadSafeBTreeDisk<std::uint64_t, std::uint64_t> btree("test.bin");
  std::vector<std::thread> threads;
  std::vector<int> v(n);
  for (std::size_t i = 0; i < n; i++) {
    v[i] = i;
  }
  std::random_shuffle(v.begin(), v.end());
  for (std::size_t i = 0; i < preload_n; i++) {
    btree.insert(v[i], v[i] + 10000);
  }
  btree.direct_open("test.bin");
  auto start = std::chrono::high_resolution_clock::now();
  for (std::size_t i = 0; i < num_threads; i++) {
    threads.emplace_back([&, i]() {
      auto block_size = (n - preload_n + num_threads - 1) / num_threads;
      auto begin = preload_n + i * block_size;
      auto end = preload_n + (i + 1) * block_size;
      for (std::size_t j = begin; j < end && j < n; j++) {
        btree.insert(v[j], v[j] + 10000);
      }
    });
  }
  for (auto &t : threads) {
    t.join();
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
  std::cout << "num_threads," << num_threads<<", insert time," << ns / 1e6 << "ns,speed," << (double)(n - preload_n) / ns * 1e9
            << " ops/s,"<<std::endl;
  btree.no_direct_open("test.bin");
  for (std::size_t j = 0; j < n; j++) {
    assert(btree.lookup(j) == j + 10000);
  }
  btree.is_valid();
}

int main() {
  const std::size_t preload_n = 1e6;
  const std::size_t n = preload_n + 1e4;
  // ThreadSafeBTreeDisk<std::uint64_t, std::uint64_t> btree("test.bin");
  multi_thread_test(n, 1, preload_n);
  multi_thread_test(n, 2, preload_n);
  multi_thread_test(n, 4, preload_n);
  multi_thread_test(n, 8, preload_n);
  multi_thread_test(n, 16, preload_n);
  multi_thread_test(n, 32, preload_n);
}
