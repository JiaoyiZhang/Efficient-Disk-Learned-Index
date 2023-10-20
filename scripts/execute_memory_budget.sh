printf "Execute microbenchmark for memory budget\n"

# workload setup
data="fb_200M_uint64"
wl="c"
pattern="/uniform_150"
page_bytes=4096
range=0
useless=1
# output date
date=${3}_memory_budget

budgets=(2 4 8 16 32) # MiB
budgets=(32) # MiB

# params of index
di=(1.02 1.5 2 2.5 3 4 5)
leco=(1 2 3 4 5)
static_li=(16 128 256 384 512 768 1024)

# # run
# hybrid_DI=(HYBRID_BTREE_DI)
# hybrid_Leco=(HYBRID_BTREE_LECO)
hybrid_baseline=(HYBRID_BTREE_PGM HYBRID_BTREE_RS)
hybrid_baseline=(HYBRID_BTREE_RS)
ft=64

index_path="$1/indexes/"

printf "Testing ${data} using workload $wl...\n"
if [ ! -d "$2/results/" ]; then
    mkdir $2/results/
fi
if [ ! -d "$2/results$pattern/" ]; then
    mkdir $2/results$pattern/
fi

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
    for index in ${hybrid_baseline[*]}
    do
        for p in ${static_li[*]}
        do
            indexfile="${index_path}${index}_${data}_${p}_$wl.idx"
            echo "    index: $index, resfile: $resfile"
            ./build/HYBRID-LID $workload_dir $range $index $useless $p $indexfile $page_bytes $budget >> $resfile
        done
    done
done

printf "The test is done, and the results is written to $resfile\n"