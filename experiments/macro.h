#ifndef MACRO_H
#define MACRO_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif  // _GNU_SOURCE

// #define TEST_SEARCH // test the correction of lookup
// #define PRINT_INFO
// #define PRINT_PAGE_STATS  // print the avg page
// #define PROF_CPU_IO

#define DIRECT_IO  // use direct io or mmap

#ifdef MULTI_THREAD
#include <pthread.h>
#define NUM_THREADS 1  // # of threads
#else
#define NUM_THREADS 1  // single thread
#endif

#define ALLOCATED_BUF_SIZE 10  // #pages (the size of buffer)

#define LAST_MILE_SEARCH 0  // 0: binary search, 1: linear search

#define MAX_NUM_QUALIFYING 100  // Consistent with SOSD.

/**
 * @brief 0 (aligned-compression mode): each logical block is aligned to 4096;
 * 1 (sequential-compression mode): each key is stored contiguously, and the
 * length of each payload is the same in a block, but different in different
 * blocks.
 */
#define ALIGNED_COMPRESSION 1
#if ALIGNED_COMPRESSION == 1
#define MAX_PAYLOAD_LENGTH 32
#endif

#include <iostream>

void PrintMacro() {
  std::cout << "---------PRINT MACRO-------------\n";
#ifdef MULTI_THREAD
  std::cout << "Use [multiple threads]: " << NUM_THREADS << std::endl;
#else
  std::cout << "[Single threads]" << std::endl;
#endif  // MULTI_THREAD

#ifdef DIRECT_IO
  std::cout << "Use [direct IO] to fetch pages on disk." << std::endl;
#else
  std::cout << "Use [mmap] to fetch pages on disk." << std::endl;
#endif  // DIRECT_IO

#if LAST_MILE_SEARCH == 0
  std::cout << "Use [binary search] to perform last-mile search." << std::endl;
#else
  std::cout << "Use [linear search] to perform last-mile search." << std::endl;
#endif  // LAST_MILE_SEARCH

#ifdef TEST_SEARCH
  std::cout << "[Test the lookup function] before evaluation." << std::endl;
#else
  std::cout << "Start evaluation [directly]." << std::endl;
#endif  // TEST_SEARCH

#if ALIGNED_COMPRESSION == 1
  std::cout
      << "If the compression mode is equal to 1, then the mode is the "
         "[sequential-compression mode]: each key is stored contiguously, "
         "and the length of each payload is the same in a block, but "
         "different in different blocks."
      << std::endl;
#else
  std::cout
      << "If the compression mode is equal to 1, then the mode is the "
         "[aligned-compression mode]: each logical block is aligned to 4096"
      << std::endl;
#endif  // ALIGNED_COMPRESSION

  std::cout << "[MAX_NUM_QUALIFYING] in memory:" << MAX_NUM_QUALIFYING
            << std::endl;
  std::cout << "---------PRINT MACRO COMPLETED-------------\n";
}

#endif  // MACRO_H