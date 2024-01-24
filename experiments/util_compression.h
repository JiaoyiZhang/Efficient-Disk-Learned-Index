/**
 * @file util_compression.h
 * @author Making In-Memory Learned Indexes Efficient on Disk
 * @brief Functions for compression mode (the sizes of pages/blocks are
 * different)
 * @version 1
 * @date 2022-10-31
 *
 * @copyright Copyright (c) 2022
 *
 */
#ifndef EXPERIMENTS_UTIL_COMPRESSION_H_
#define EXPERIMENTS_UTIL_COMPRESSION_H_

#include "util_search.h"

template <typename K>
inline ResultInfo<K> CompressionCoreLookup(const SearchRange& range,
                                           const K& lookupkey,
                                           const Params<K>& params,
                                           uint64_t gap_cnt) {
  ResultInfo<K> res_info;
  auto seek_table_res =
      params.comp_block_bytes.GetBlockRange(range.start, range.stop);
  CompressedBlockRange start_range = seek_table_res.first;
  auto new_gap_cnt = gap_cnt;

  // Fetch all the pages of this logical block
  const auto kPageSize = getpagesize();
  while (start_range.block_bytes + start_range.offset <=
         seek_table_res.second) {
    uint64_t fetch_page_num = start_range.block_eid - start_range.block_sid + 1;
    auto fd = params.open_files.find(0)->second;
    DirectIORead<K>(fd, kPageSize, fetch_page_num,
                    start_range.block_sid * kPageSize, params.read_buf_);

#if ALIGNED_COMPRESSION == 0
    auto io_offset = 0;
    uint64_t idx = LastMileSearch(
        pages_data + io_offset, start_range.block_bytes / params.record_bytes_,
        new_gap_cnt, lookupkey);
    res_info.total_search_range += start_range.block_bytes /
                                   params.record_bytes_ * sizeof(K) *
                                   new_gap_cnt;
#else
    new_gap_cnt =
        start_range.block_bytes / params.record_num_per_page_ / sizeof(K);
    auto io_offset =
        (start_range.offset - start_range.block_sid * kPageSize) / sizeof(K);
    uint64_t idx =
        LastMileSearch(params.read_buf_ + io_offset,
                       params.record_num_per_page_, new_gap_cnt, lookupkey);
    res_info.total_search_range +=
        params.record_num_per_page_ * sizeof(K) * new_gap_cnt;
#endif
    res_info.fetch_page_num += fetch_page_num;
    res_info.res = *(params.read_buf_ + io_offset + idx * new_gap_cnt);
    if (res_info.res == lookupkey) {
      return res_info;
    }
    start_range =
        params.comp_block_bytes.GetNextBlockRange(start_range.idx + 1);
  }

  return res_info;
}
#endif  // EXPERIMENTS_UTIL_COMPRESSION_H_