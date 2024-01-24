printf "Execute multi-threaded YCSB evaluations (baseline)\n"

# workload setup
dataset=(fb_200M_uint64)
workloads=(cc aa c)
patterns=("/uniform_150")
page_bytes=4096
range=0
# output date
date=$3
thread_num=$4


# # run
useless=64

index_path="$1/indexes/"
for pattern in ${patterns[*]}
do
    rm -rf $index_path
    mkdir $index_path
    for data in ${dataset[*]}
    do
        for wl in ${workloads[*]}
        do
            printf "Testing ${data} using workload $wl...\n"
            if [ ! -d "$2/results/" ]; then
                mkdir $2/results/
            fi
            if [ ! -d "$2/results$pattern/" ]; then
                mkdir $2/results$pattern/
            fi
            workload_dir="$1$pattern/$data/$wl"
            
            resfile="$2/results$pattern/${date}_${data}_${wl}_baseline.csv"
            
            index="BTREE"
            indexfile="${index_path}${index}_${data}_${wl}_${thread_num}.idx"
            echo "     index: $index, resfile: $resfile, indexfile: $indexfile"
            ./build/MULTI-HYBRID-LID $workload_dir $range $index $useless $useless $indexfile $page_bytes $thread_num >> $resfile
            
            printf "The test is done, and the results is written to $resfile\n"
        done
    done
done