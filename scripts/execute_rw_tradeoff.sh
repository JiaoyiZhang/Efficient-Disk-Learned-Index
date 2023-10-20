printf "Execute microbenchmark for memory budget\n"

# workload setup
data="fb_200M_uint64"
wl="aa"
pattern="/uniform_150"
page_bytes=4096
range=0
useless=1
# output date
date=${3}_rw_tradeoff

budget=`expr 10 \* 1024 \* 1024`

# # params of index
# di=(1.02 1.5 2 2.5 3 4 5)
# leco=(1 2 3 4 5)
# static_li=(16 128 256 384 512 768 1024)
# params of index
di=(6 7)
leco=(6 7)
static_li=(1280 1536)

# # run
# hybrid_DI=(HYBRID_BTREE_DI)
hybrid_Leco=(HYBRID_BTREE_LECO)
hybrid_baseline=(HYBRID_BTREE_PGM HYBRID_BTREE_RS)
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

rm -rf $index_path
mkdir $index_path
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

printf "The test is done, and the results is written to $resfile\n"