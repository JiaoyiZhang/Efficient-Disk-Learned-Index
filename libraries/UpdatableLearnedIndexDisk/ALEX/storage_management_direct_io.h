#pragma once

#include <fcntl.h>
#include <math.h>
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
namespace alex_disk {
#define Caching 0
const long AlexBlockSize = 8192L;
typedef struct {
  int next_block;
  int next_offset;
  int root_block_id;
  int root_offset;
  // used in bulk node
  int last_data_node_block;
  int last_data_node_offset;
  //
  int left_most_block;
  int left_most_offset;
  int right_most_block;
  int right_most_offset;
  int is_leaf;
  uint8_t dup_left;
  uint8_t dup_right;
  uint8_t dup_root;
} MetaNode;

typedef struct {
  char data[AlexBlockSize];
} Block;
#define MetaNodeSize sizeof(MetaNode)
class StorageManager {
 private:
  //   char *file_name = nullptr;
  std::string filename;
  //   FILE *fp = nullptr;
  int fd;

  void _write_block(void *data, int block_id) {
    if (lseek(fd, block_id * AlexBlockSize, SEEK_SET) == -1) {
      throw std::runtime_error("lseek file error in _write_block");
    }
    void *buf = aligned_alloc(AlexBlockSize, AlexBlockSize);
    memcpy(buf, data, AlexBlockSize);
    int ret = write(fd, buf, AlexBlockSize);
    if (ret == -1) {
      throw std::runtime_error("write error in DirectIOWrite");
    }
    return;
  }

  char *_allocate_new_block() {
    char *ptr = nullptr;
    ptr = (char *)malloc(AlexBlockSize * sizeof(char));
    return ptr;
  }

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

  void _create_file() {
    fd = open(filename.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0644);
    void *empty_block = aligned_alloc(AlexBlockSize, AlexBlockSize);
    for (int i = 0; i < AlexBlockSize; i++) {
      *(static_cast<char *>(empty_block) + i) = 0;
    }
    MetaNode mn;
    mn.next_block = 1;
    mn.next_offset = 0;
    mn.root_block_id = -1;
    mn.root_offset = -1;
    mn.last_data_node_block = -1;
    mn.last_data_node_offset = -1;
    // mn.level = 1;
    memcpy(empty_block, &mn, MetaNodeSize);
    _write_block(empty_block, 0);
    _close_file_handle();
    _get_file_handle();
  }

  void _close_file_handle() { close(fd); }

 public:
  StorageManager(char *fn, bool first = false) {
    filename = fn;
    if (first) {
      _create_file();
    } else {
      _get_file_handle();
    }
  }

  StorageManager() = default;

  ~StorageManager() {
    if (fd != -1) _close_file_handle();
  }

  size_t get_file_size() {
    struct stat file_stat;
    auto res = fstat(fd, &file_stat);
    if (res != 0) {
      std::cout << "get file size wrong:" << res << std::endl;
    }
    return file_stat.st_size;
  }

  void _read_block(void *data, int block_id) {
    if (lseek(fd, block_id * AlexBlockSize, SEEK_SET) == -1) {
      throw std::runtime_error("lseek file error in _read_block");
    }
    void *buf = aligned_alloc(AlexBlockSize, AlexBlockSize);
    int ret = read(fd, buf, AlexBlockSize);
    if (ret == -1) {
      throw std::runtime_error("read error in DirectIORead");
    }
    memcpy(data, buf, AlexBlockSize);
    return;
  }

  Block get_block(int block_id) {
    Block block;
    // char data[AlexBlockSize];
    _read_block(block.data, block_id);
    // memcpy(block.data, data, AlexBlockSize);
    return block;
  }

  void get_block(int block_id, char *data) { _read_block(data, block_id); }

  void write_block(int block_id, Block block) {
    _write_block(&block, block_id);
  }

  void write_with_size(int block_id, void *data, long size) {
    if (lseek(fd, block_id * AlexBlockSize, SEEK_SET) == -1) {
      throw std::runtime_error("lseek file error in write_with_size");
    }
    write(fd, data, size);
    return;
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

  void read_block_arbitrary(void *data, long offset) {
    if (lseek(fd, offset, SEEK_SET) == -1) {
      throw std::runtime_error("lseek file error in read_block_arbitrary");
    }
    void *buf = aligned_alloc(AlexBlockSize, AlexBlockSize);
    read(fd, buf, AlexBlockSize);
    return;
  }

  void read_arbitrary(void *data, long offset, long len) {
    if (lseek(fd, offset, SEEK_SET) == -1) {
      throw std::runtime_error("lseek file error in read_arbitrary");
    }
    size_t num = std::ceil(len * 1.0 / AlexBlockSize);
    void *buf = aligned_alloc(AlexBlockSize, AlexBlockSize * num);
    read(fd, buf, AlexBlockSize * num);
    return;
   }
};
}  // namespace alex_disk