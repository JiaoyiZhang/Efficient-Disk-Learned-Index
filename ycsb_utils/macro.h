#ifndef MACRO_H
#define MACRO_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif  // _GNU_SOURCE

#define PRINT_MIB(bytes) (bytes) / 1024.0 / 1024.0

// #define CHECK_CORRECTION
// #define PRINT_MULTI_THREAD_INFO
// #define PRINT_PROCESSING_INFO
// #define BREAKDOWN
#define HYBRID_MODE 1  // for baseline, inner nodes are stored in main memory

#include <iostream>

#endif  // MACRO_H