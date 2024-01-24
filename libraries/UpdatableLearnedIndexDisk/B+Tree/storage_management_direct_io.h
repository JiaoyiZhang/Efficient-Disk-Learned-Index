#pragma once

#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <map>

#include "utility.h"

#define Caching 0

class StorageManager {
 private:
  //   char *file_name = nullptr;
  std::string filename;
  //   FILE *fp = nullptr;
  int fd;
  void *buf = aligned_alloc(BlockSize, 4 * BlockSize);
#if Caching
  std::map<int, Block> block_cache;  // LRU setting?
#endif

  void _write_block(void *data, int block_id) {
    if (lseek(fd, block_id * BlockSize, SEEK_SET) == -1) {
      throw std::runtime_error("lseek file error in _write_block");
    }
    memcpy(buf, data, BlockSize);
    int ret = write(fd, buf, BlockSize);
    if (ret == -1) {
      throw std::runtime_error("write error in DirectIOWrite");
    }
    return;
  }

  void _read_block(void *data, int block_id) {
    if (lseek(fd, block_id * BlockSize, SEEK_SET) == -1) {
      throw std::runtime_error("lseek file error in _read_block");
    }
    int ret = read(fd, buf, BlockSize);
    if (ret == -1) {
      throw std::runtime_error("read error in DirectIORead");
    }
    memcpy(data, buf, BlockSize);
    return;
  }

  // char *_allocate_new_block() {
  //   char *ptr = nullptr;
  //   // ptr = (char *)malloc(BlockSize * sizeof(char));
  //   ptr = (char *)aligned_alloc(BlockSize, BlockSize);
  //   return ptr;
  // }

  void _get_file_handle() {
#ifdef __APPLE__
    // Reference:
    // https://github.com/facebook/rocksdb/wiki/Direct-IO
    fd = open(filename.c_str(), O_RDWR);
    fcntl(fd, F_NOCACHE, 1);
#else  // Linux
    fd = open(filename.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0644);
#endif
    if (fd == -1) {
      throw std::runtime_error("open file error in _get_file_handle");
    }
  }

  void _create_file(bool bulk) {
    fd = open(filename.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0644);
    MetaNode mn;
    mn.block_count = 2;
    mn.root_block_id = 1;
    mn.level = 1;
    // Making In-Memory Learned Indexes Efficient on Disk
    mn.last_block = 0;
    memcpy(buf, &mn, MetaNodeSize);
    _write_block(buf, 0);
    if (!bulk) {
      LeaftNodeHeader lnh;
      lnh.item_count = 0;
      lnh.node_type = LeafNodeType;
      lnh.level = 1;  // level starts from 1
      memcpy(buf, &lnh, LeaftNodeHeaderSize);
      _write_block(buf, 1);
    }
    _close_file_handle();
    _get_file_handle();
  }

  void _close_file_handle() { close(fd); }

 public:
  StorageManager(char *fn, bool first = false, bool bulk_load = false) {
    filename = fn;
    if (first) {
      _create_file(bulk_load);
    } else {
      _get_file_handle();
    }
  }

  StorageManager(bool first, char *fn) {
    filename = fn;

    if (first) {
      fd = open(filename.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0644);

      MetaNode mn;
      mn.block_count = 1;
      mn.level = 0;
      memcpy(buf, &mn, MetaNodeSize);
      _write_block(buf, 0);

      _close_file_handle();
    }
    _get_file_handle();
  }

  StorageManager() = default;

  ~StorageManager() {
    if (fd != -1) _close_file_handle();
  }

  Block get_block(int block_id) {
    Block block;
    // char data[BlockSize];
    _read_block(block.data, block_id);
    // memcpy(block.data, data, BlockSize);
    return block;
  }

  void get_block(int block_id, char *data) { _read_block(data, block_id); }

  void write_block(int block_id, Block block) {
    _write_block(&block, block_id);
  }

  void write_with_size(int block_id, void *data, long size) {
    // fseek(fp, block_id * BlockSize, SEEK_SET);
    // fwrite(data, size, 1, fp);
    // return;
    if (lseek(fd, block_id * BlockSize, SEEK_SET) == -1) {
      throw std::runtime_error("lseek file error in write_with_size");
    }
    write(fd, data, size);
    return;
  }

  size_t get_file_size() {
    struct stat file_stat;
    auto res = fstat(fd, &file_stat);
    if (res != 0) {
      std::cout << "get file size wrong:" << res << std::endl;
    }
    return file_stat.st_size;
  }

  void write_arbitrary(long offset, void *data, long size) {
    // fseek(fp, offset, SEEK_SET);
    // fwrite(data, size, 1, fp);
    if (lseek(fd, offset, SEEK_SET) == -1) {
      throw std::runtime_error("lseek file error in write_arbitrary");
    }
    write(fd, data, size);
    return;
  }
};