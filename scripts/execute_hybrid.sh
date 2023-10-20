printf "Execute microbenchmark\n"
# workload setup
dataset=(fb_200M_uint64 books_200M_uint64 osm_cellids_200M_uint64)
workloads=(c cc aa)
patterns=("/uniform_150")
page_bytes=4096
useless=1
range=0
dynamic_pgm=64
# output date
date=$3

# # params of hybrid
budgetA=10265559 # 9.79 MiB
budgetB=8598323  # 8.2 MiB for alex
budget=$budgetA

# # run
hybrid_DI=(HYBRID_BTREE_DI)
hybrid_Leco=(HYBRID_BTREE_LECO)
ft=64

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
            if [ "$wl" = "e" ];then
                range=1
            else
                range=0
            fi
            workload_dir="$1$pattern/$data/$wl"
            
            resfile="$2/results$pattern/${date}_${data}_${wl}.csv"
            for index in ${hybrid_DI[*]}
            do
                if [ "$index" = "HYBRID_ALEX_DI" ];then
                    tmpBudget=$budgetB
                else
                    tmpBudget=$budget
                fi
                indexfile="${index_path}${index}_${data}_${wl}.idx"
                for p in ${di[*]}
                do
                    echo "     index: $index, resfile: $resfile"
                    ./build/HYBRID-LID $workload_dir $range $index $useless $p $indexfile $page_bytes $tmpBudget >> $resfile
                done
            done
            
            for index in ${hybrid_Leco[*]}
            do
                if [ "$index" = "HYBRID_ALEX_LECO" ];then
                    tmpBudget=$budgetB
                else
                    tmpBudget=$budget
                fi
                indexfile="${index_path}${index}_${data}_$wl.idx"
                for p in ${leco[*]}
                do
                    echo "     index: $index, resfile: $resfile"
                    ./build/HYBRID-LID $workload_dir $range $index $useless $p $indexfile $page_bytes $tmpBudget >> $resfile
                done
            done
            
            for index in ${hybrid_baseline[*]}
            do
                if [ "$index" = "HYBRID_ALEX_PGM" ] || [ "$index" = "HYBRID_ALEX_RS" ];then
                    tmpBudget=$budgetB
                else
                    tmpBudget=$budget
                fi
                indexfile="${index_path}${index}_${data}_$wl.idx"
                for p in ${static_li[*]}
                do
                    actualP=`expr $p / 2`
                    echo "    index: $index, resfile: $resfile"
                    ./build/HYBRID-LID $workload_dir $range $index $useless $actualP $indexfile $page_bytes $tmpBudget >> $resfile
                done
            done
            
            printf "The test is done, and the results is written to $resfile\n"
        done
    done
done