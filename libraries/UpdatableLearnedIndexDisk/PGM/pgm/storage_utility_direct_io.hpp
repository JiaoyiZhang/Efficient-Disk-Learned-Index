#ifndef PGM_DIRECT_IO_HPP
#define PGM_DIRECT_IO_HPP
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
namespace pgm_baseline_disk {
const long BLOCK_SIZE = 8192 / 2;

void write_data(int fd, void *data, long offset, int len) {
  if (lseek(fd, offset, SEEK_SET) == -1) {
    throw std::runtime_error("lseek file error in _write_block");
  }
  int cnt = std::ceil(len * 1.0 / BLOCK_SIZE);
  int ret = write(fd, data, BLOCK_SIZE * cnt);
  if (ret == -1) {
    throw std::runtime_error("write error in DirectIOWrite");
  }
  return;
}

template <typename ItemOnDisk>
void write_block(int fd, void *data, int block_id, int len, bool in_memory,
                 ItemOnDisk *level_memory_data) {
  int cnt = std::ceil(len * 1.0 * sizeof(ItemOnDisk) / BLOCK_SIZE);
  if (in_memory) {
    int offset = (block_id * BLOCK_SIZE) / sizeof(ItemOnDisk);
    memcpy(level_memory_data + offset, data, sizeof(ItemOnDisk) * len);
  } else {
    int total_num = cnt;
    while (total_num > 0) {
      int tmp_num = total_num;
      if (tmp_num > 500000) {
        tmp_num = 500000;
      }
      size_t offset = BLOCK_SIZE * (cnt - total_num);
      if (lseek(fd, block_id * BLOCK_SIZE, SEEK_SET) == -1) {
        throw std::runtime_error("lseek file error in _write_block");
      }

      int ret = write(fd, data + offset, BLOCK_SIZE * tmp_num);
      if (ret == -1) {
        throw std::runtime_error("write error in DirectIOWrite");
      }
      total_num -= tmp_num;
    }
  }
  return;
}

void read_block(int fd, void *data, int block_id) {
  if (lseek(fd, block_id * BLOCK_SIZE, SEEK_SET) == -1) {
    throw std::runtime_error("lseek file error in _read_block");
  }
  int ret = read(fd, data, BLOCK_SIZE);
  if (ret == -1) {
    throw std::runtime_error("read error in DirectIORead");
  }
  return;
}

template <typename ItemOnDisk>
void read_block(int fd, void *data, int block_id, bool in_memory,
                ItemOnDisk *level_memory_data) {
  if (in_memory) {
    int offset = (block_id * BLOCK_SIZE) / sizeof(ItemOnDisk);
    memcpy(data, level_memory_data + offset, BLOCK_SIZE);
  } else {
    if (lseek(fd, block_id * BLOCK_SIZE, SEEK_SET) == -1) {
      throw std::runtime_error("lseek file error in _read_block");
    }
    int ret = read(fd, data, BLOCK_SIZE);
    if (ret == -1) {
      throw std::runtime_error("read error in DirectIORead");
    }
  }
  return;
}

template <typename ItemOnDisk>
void write_block(int fd, void *data, int block_id, bool in_memory,
                 ItemOnDisk *level_memory_data) {
  if (in_memory) {
    int offset = (block_id * BLOCK_SIZE) / sizeof(ItemOnDisk);
    memcpy(level_memory_data + offset, data, BLOCK_SIZE);
  } else {
    if (lseek(fd, block_id * BLOCK_SIZE, SEEK_SET) == -1) {
      throw std::runtime_error("lseek file error in _write_block");
    }
    int ret = write(fd, data, BLOCK_SIZE);
    if (ret == -1) {
      throw std::runtime_error("write error in DirectIOWrite");
    }
  }
  return;
}

void write_block(int fd, void *data, int block_id) {
  if (lseek(fd, block_id * BLOCK_SIZE, SEEK_SET) == -1) {
    throw std::runtime_error("lseek file error in _write_block");
  }
  int ret = write(fd, data, BLOCK_SIZE);
  if (ret == -1) {
    throw std::runtime_error("write error in DirectIOWrite");
  }
  return;
}

void read_data(int fd, void *data, long offset, int len) {
  int cnt = std::ceil(len * 1.0 / BLOCK_SIZE);
  if (lseek(fd, offset, SEEK_SET) == -1) {
    throw std::runtime_error("lseek file error in _read_block");
  }
  int ret = read(fd, data, BLOCK_SIZE * cnt);
  if (ret == -1) {
    throw std::runtime_error("read error in DirectIORead");
  }
  return;
}

template <typename ItemOnDisk>
void read_data(int fd, void *data, int block_id, int len, bool in_memory,
               ItemOnDisk *level_memory_data) {
  int cnt = std::ceil(len * 1.0 * sizeof(ItemOnDisk) / BLOCK_SIZE);
  if (in_memory) {
    int offset = (block_id * BLOCK_SIZE) / sizeof(ItemOnDisk);
    memcpy(data, level_memory_data + offset, sizeof(ItemOnDisk) * len);
  } else {
    if (lseek(fd, block_id * BLOCK_SIZE, SEEK_SET) == -1) {
      throw std::runtime_error("lseek file error in _read_block");
    }
    int ret = read(fd, data, BLOCK_SIZE * cnt);
    if (ret == -1) {
      throw std::runtime_error("read error in DirectIORead");
    }
  }
  return;
}
}  // namespace pgm_baseline_disk

#endif