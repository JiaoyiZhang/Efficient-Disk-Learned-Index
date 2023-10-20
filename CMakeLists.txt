cmake_minimum_required(VERSION 3.16.2)
project(LID)
set(CMAKE_CXX_STANDARD 17)

option(USE_MULTI_THREAD "Use multi-thread" ON)

if(USE_MULTI_THREAD)
    add_compile_definitions(MULTI_THREAD)
endif()

if(MSVC)
    set(CMAKE_CXX_FLAGS "/arch:AVX2 /W1 /EHsc")
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Intel")
    set(CMAKE_CXX_FLAGS "-xHost")
else()
    # clang and gcc
    include(CheckCXXCompilerFlag)
    check_cxx_compiler_flag("-march=native" HAS_MARCH_NATIVE)
    if (HAS_MARCH_NATIVE)
        add_compile_options(-march=native)
    endif ()
    add_compile_options(-Wall -Wextra)
endif()

add_library(pgm_index INTERFACE)
target_include_directories(pgm_index INTERFACE indexes/PGM-index/include)

# .h document
include_directories(libraries/LeCo/headers /usr/include/eigen3)
add_subdirectory(libraries/LeCo/src)
add_subdirectory(libraries/LeCo/thirdparty/fsst)
# .c/cpp document
find_package(Snappy)
aux_source_directory(libraries/LeCo/experiments SRC_LIST)
add_library(leco ${SRC_LIST})
target_link_libraries(leco docs Snappy::snappy gmp gmpxx fsst)

add_executable(prepare_ycsb gen_ycsb_workloads.cpp)
add_executable(HYBRID-LID run_ycsb_experiments.cpp)
add_executable(LID run_microbenchmark.cpp)

if (APPLE)
    # OpenMP disabled on Mac since m1 use osx-arm64 library
else()
    find_package(OpenMP)
    if (OpenMP_CXX_FOUND)
        message(STATUS "OpenMP found")
        target_link_libraries(pgm_index INTERFACE OpenMP::OpenMP_CXX)
        target_link_libraries(HYBRID-LID OpenMP::OpenMP_CXX leco)
    else()
        message(FATAL_ERROR "Openmp not found!")
        target_link_libraries(HYBRID-LID
            PRIVATE leco
        )
    endif ()
endif()

target_link_libraries(LID
    PRIVATE pgm_index
    PRIVATE leco
)

if (APPLE)
    # PGM-index requires x86
    set_target_properties(LID PROPERTIES OSX_ARCHITECTURES x86_64)
    set_target_properties(HYBRID-LID PROPERTIES OSX_ARCHITECTURES x86_64)
    # POPCNT is required by ALEX
    target_compile_options(LID PRIVATE -march=x86-64-v2)
    target_compile_options(HYBRID-LID PRIVATE -march=x86-64-v2)
endif()
