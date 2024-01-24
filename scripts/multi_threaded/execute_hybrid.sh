printf "Execute multi-threaded ycsb benchmark\n"
# workload setup
dataset=(fb_200M_uint64)
workloads=(cc aa c)
patterns=("/uniform_150")
page_bytes=4096
useless=1
range=0
# output date
date=$3
thread_num=$4
merge_t_num=$5

# # params of hybrid
# budgetA=10265559 # 9.79 MiB
# budgetB=8598323  # 8.2 MiB for alex
# budget=$budgetA
merge_ratio=178

# # run
hybrid_DI=(HYBRID_BTREE_DI)
hybrid_Leco=(HYBRID_BTREE_LECO)

index_path="$1/indexes/"
for pattern in ${patterns[*]}
do
    rm -rf $index_path
    mkdir $index_path
    for wl in ${workloads[*]}
    do
        source scripts/params_${wl}
        for data in ${dataset[*]}
        do
            printf "Testing ${data} using workload $wl...\n"
            if [ ! -d "$2/results/" ]; then
                mkdir $2/results/
            fi
            if [ ! -d "$2/results$pattern/" ]; then
                mkdir $2/results$pattern/
            fi
            workload_dir="$1$pattern/$data/$wl"
            
            resfile="$2/results$pattern/${date}_${data}_${wl}.csv"
            for index in ${hybrid_DI[*]}
            do
                indexfile="${index_path}${index}_${data}_${wl}.idx"
                for p in ${di[*]}
                do
                    echo "     index: $index, resfile: $resfile"
                    ./build/MULTI-HYBRID-LID $workload_dir $range $index $useless $p $indexfile $page_bytes $thread_num $merge_ratio $merge_t_num >> $resfile
                done
            done
            
            for index in ${hybrid_Leco[*]}
            do
                indexfile="${index_path}${index}_${data}_$wl.idx"
                for p in ${leco[*]}
                do
                    echo "     index: $index, resfile: $resfile"
                    ./build/MULTI-HYBRID-LID $workload_dir $range $index $useless $p $indexfile $page_bytes $thread_num $merge_ratio $merge_t_num >> $resfile
                done
            done
            
            printf "The test is done, and the results is written to $resfile\n"
        done
    done
done