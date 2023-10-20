printf "Execute microbenchmark\n

# workload setup
dataset=(fb_200M_uint64 books_200M_uint64 osm_cellids_200M_uint64)
workloads=(cc aa c)
patterns=("/uniform_150")
page_bytes=4096
range=0
# output date
date=$3

budget=10265559 # 9.79 MiB

# # run
bt_inner_disk=(2 1 0) # for BTREE
pgm_params=(16 256 512 768 1024) # for PGM_ORIGIN
useless=64

index_path="$1/indexes/"
for pattern in ${patterns[*]}
do
    rm -rf $index_path
    mkdir $index_path
    if [ "$pattern" = "/zipfian" ];then
        workloads=(a c)
    fi
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
            if [ "$wl" = "e" ];then
                range=1
            else
                range=0
            fi
            workload_dir="$1$pattern/$data/$wl"

            resfile="$2/results$pattern/${date}_${data}_${wl}_baseline.csv"
            for bt in ${bt_inner_disk[*]}
            do
                index="BTREE"
                indexfile="${index_path}${index}_${data}_${wl}_$bt.idx"
                echo "     index: $index, resfile: $resfile"
                ./build/HYBRID-LID $workload_dir $range $index $bt $useless $indexfile $page_bytes $budget >> $resfile
            done
            for pgm in ${pgm_params[*]}
            do
                index="PGM_ORIGIN"
                indexfile="${index_path}${index}_${data}_${wl}_$pgm.idx"
                echo "     index: $index, resfile: $resfile"
                ./build/HYBRID-LID $workload_dir $range $index $pgm $useless $indexfile $page_bytes $budget >> $resfile
            done

            printf "The test is done, and the results is written to $resfile\n"
        done
    done
done