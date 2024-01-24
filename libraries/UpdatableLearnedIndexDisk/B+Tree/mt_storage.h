#pragma once

#include <fcntl.h>
#include <unistd.h>

#include <stdexcept>
#include <string>

#include "utility.h"

class ThreadSafeStorageManager {
 public:
  int fd;

 public:
  ThreadSafeStorageManager() : fd(-1) {}
  ThreadSafeStorageManager(std::string filename)
      : fd(open(filename.c_str(), O_RDWR | O_CREAT /*| O_DIRECT*/, 0644)) {
    if (fd == -1) {
      throw std::runtime_error("open error");
    }
  }
  void close_file() {
    if (fd != -1) {
      close(fd);
    }
  }

  void direct_open(std::string filename) {
    if (fd != -1) {
      close(fd);
    }
    fd = open(filename.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0644);
  }

  void no_direct_open(std::string filename) {
    if (fd != -1) {
      close(fd);
    }
    fd = open(filename.c_str(), O_RDWR | O_CREAT, 0644);
  }

  static void *alloc() { return std::aligned_alloc(BlockSize, BlockSize); }

  void write_block(std::size_t block_id, void *data) {
    auto n = pwrite(fd, data, BlockSize, block_id * BlockSize);
    if (n != BlockSize) {
      throw std::runtime_error("write_block error");
    }
  }

  void read_block(std::size_t block_id, void *data) const {
    auto n = pread(fd, data, BlockSize, block_id * BlockSize);
    if (n != BlockSize) {
      throw std::runtime_error("read_block error");
    }
  }
};
