bash scripts/build_benchmark.sh ON

function prepareYCSB(){
    src="../../../../../$1/SOSD/"
    resfile="./results/$1"
    echo "Test Disk on $1, the datasets are stored in $src, #init_key is $2, #ops is $3"
    
    bash scripts/prepare_ycsb.sh $src $2 $3 $resfile
}

function run(){
    src="../../../../$1/SOSD/hybrid"
    if [ ! -d "$src" ]; then
        mkdir $src
    fi
    resfile="./results/multi_thread/$1"
    if [ ! -d "./results/multi_thread" ]; then
        mkdir "./results/multi_thread"
    fi
    if [ ! -d "./results/multi_thread/$1" ]; then
        mkdir "./results/multi_thread/$1"
    fi
    echo "Test Disk on $1, the datasets are stored in $src, thread num: $2, merge thread num: $3"
    
    prefix=0120_final_$2
    bash scripts/multi_threaded/execute_hybrid.sh $src $resfile $prefix $2 $3
    bash scripts/multi_threaded/execute_baseline.sh $src $resfile $prefix $2 $3
}

# diskname=(optane PM9A3)
diskname=(PM9A3)
init=150 # in million
ops=10 # in million, ops+init must be less than 200M
threadnum=(32 16 8 4 2)

for disk in ${diskname[*]}
do
    # prepareYCSB $disk $init $ops
    for t_num in ${threadnum[*]}
    do
        merge_thread=`expr $t_num - 1`
        if [ "$t_num" = 1 ];then
            merge_thread=1
        fi
        run $disk $t_num $merge_thread
    done
done