# Efficient Learned Indexes on Disk
A microbenchmark and YCSB benchmark for evaluating learned indexes on disk.\
More details can be found in our paper in SIGMOD 2024: [**Making In-Memory Learned Indexes Efficient on Disk**](https://dl.acm.org/doi/pdf/10.1145/3654954).

## Supported indexes
The indexes are included as git submodules.
Add `--recursive` option when `git clone`-ing,
or run `git submodule init && git submodule update` in the local repo.

### Microbenchmark
- [RS](https://github.com/learnedsystems/RadixSpline.git)
- [PGM-index](https://github.com/gvinciguerra/PGM-index)
- Compressed PGM
- PGM_Disk
- Cpr_PGM_Disk
- CprLeCo_PGM_Disk
- Zone Map
- LeCo-Zonemap
- LeCo-Zonemap-Disk

### YCSB Benchmark
- Disk-based PGM
- Disk-based B+tree
- Hybrid_B+tree_PGM-Disk
- Hybrid_B+tree_LeCo-Disk
- Hybrid_B+tree_RS
- Hybrid_B+tree_PGM
- Hybrid_ALEX_PGM-Disk
- Hybrid_ALEX_LeCo-Disk
- Hybrid_ALEX_RS
- Hybrid_ALEX_PGM


## Running the benchmark
### Prepare datasets
Put the dataset in the `datasets` directory. You can use [SOSD datasets](https://github.com/learnedsystems/SOSD) or other datasets, as long as the data format meets:
```
key0
key1
key2
...
```

### Run Microbenchmark
Please make sure that the datasets in the corresponding scripts are already stored in `./datasets/`. The parameters in each experiment are included in each sub-script.
```bash
bash RunOnSingleDisk.sh
```

### Run YCSB Benchmark
1. Use the scripts of [index-microbench](https://github.com/huanchenz/index-microbench.git) to generate YCSB default workloads. The parameters `in workload_config.inp` are `workload name` and `monoint`. The workloads in our paper are:
    - Common Settings:
        ```bash
        recordcount=150000000
        operationcount=10000000
        requestdistribution=uniform
        # the other settings are the same in YCSB default workloads
        ```
    - Read-Only: 
        ```bash
        readproportion=0
        updateproportion=0
        scanproportion=0
        insertproportion=1
        ``` 
    - Write-Only: 
        ```bash
        readproportion=1
        updateproportion=0
        scanproportion=0
        insertproportion=0
        ``` 
    - Balanced: 
        ```bash
        readproportion=0.5
        updateproportion=0
        scanproportion=0
        insertproportion=0.5
        ``` 
2. Move the generated txn files in workloads (e.g., `txn_monoint_workloadc`) to `./ycsb_workloads/uniform_150/` in this repo.
3. Generate the YCSB workloads on existing datasets:
```bash
bash scripts/build_benchmark.sh
bash PrepareYCSB.sh
```
4. [optional] Modify the index parameters in `scripts/params_cc` (for write-only workload), `scripts/params_c` (for read-only workload), `scripts/params_aa` (for balanced workload)
5. Run Single-Threaded YCSB Benchmark
```bash
bash RunYCSBOnSingleDisk.sh
```
6. Run Multi-Threaded YCSB Benchmark
The number of threads is included in `RunMultiThreadedYCSB.sh`
```bash
bash RunMultiThreadedYCSB.sh
```
