printf "Execute microbenchmark for memory budget\n"

# workload setup
dataset=(fb_200M_uint64 books_200M_uint64 osm_cellids_200M_uint64)
workloads=(cc)
pattern="/uniform_150"
page_bytes=4096
range=0
useless=1
# output date
date=${3}_write_curve

budgets=(10 8 6 4 2) # MiB

# # run
hybrid_DI=(HYBRID_BTREE_DI)
hybrid_Leco=(HYBRID_BTREE_LECO)

index_path="$1/indexes/"

printf "Testing ${data} using workload $wl...\n"
if [ ! -d "$2/results/" ]; then
    mkdir $2/results/
fi
if [ ! -d "$2/results$pattern/" ]; then
    mkdir $2/results$pattern/
fi


for wl in ${workloads[*]}
do
    source scripts/params_${wl}
    for data in ${dataset[*]}
    do
        workload_dir="$1$pattern/$data/$wl"
        for bg in ${budgets[*]}
        do
            rm -rf $index_path
            mkdir $index_path
            budget=`expr $bg \* 1024 \* 1024`
            resfile="$2/results$pattern/${date}_${data}_${wl}.csv"
            for index in ${hybrid_DI[*]}
            do
                for p in ${di[*]}
                do
                    indexfile="${index_path}${index}_${data}_${p}_$wl.idx"
                    echo "     index: $index, resfile: $resfile"
                    ./build/HYBRID-LID $workload_dir $range $index $useless $p $indexfile $page_bytes $budget >> $resfile
                done
            done
            
            for index in ${hybrid_Leco[*]}
            do
                for p in ${leco[*]}
                do
                    indexfile="${index_path}${index}_${data}_${p}_$wl.idx"
                    echo "    index: $index, resfile: $resfile"
                    ./build/HYBRID-LID $workload_dir $range $index $useless $p $indexfile $page_bytes $budget >> $resfile
                done
            done
        done
    done
done

printf "The test is done, and the results is written to $resfile\n"