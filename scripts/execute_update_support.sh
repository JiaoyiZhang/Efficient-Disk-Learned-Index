printf "Execute microbenchmark for update support\n"

# workload setup
data="fb_200M_uint64"
wl="aa"
pattern="/uniform_150"
page_bytes=4096
range=0
useless=1
# output date
date=${3}_update

budget=9437184 # 9 MiB

# params of index
di=(1.5)
static_li=(128)

# # run
hybrid_DI=(HYBRID_ALEX_DI)
hybrid_baseline=(HYBRID_ALEX_PGM)
baseline=(BTREE)
ft=64

index_path="$1/indexes/"
rm -rf $index_path
mkdir $index_path

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
        ./build/HYBRID-LID $workload_dir $range $index $useless $p $indexfile $page_bytes $budget >> $resfile
    done
done

for index in ${hybrid_baseline[*]}
do
    indexfile="${index_path}${index}_${data}_$wl.idx"
    for p in ${static_li[*]}
    do
        echo "    index: $index, resfile: $resfile"
        ./build/HYBRID-LID $workload_dir $range $index $useless $p $indexfile $page_bytes $budget >> $resfile
    done
done

resfile="$2/results$pattern/${date}_${data}_${wl}_baseline.csv"
for index in ${baseline[*]}
do
    indexfile="${index_path}${index}_${data}_$wl.idx"
    echo "     index: $index, resfile: $resfile"
    ./build/HYBRID-LID $workload_dir $range $index $ft $ft $indexfile $page_bytes $budget >> $resfile
done

printf "The test is done, and the results is written to $resfile\n"