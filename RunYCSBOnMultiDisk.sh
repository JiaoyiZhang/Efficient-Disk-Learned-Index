bash scripts/build_benchmark.sh ON

function prepareYCSB(){
    src="../../../../$1/SOSD/"
    resfile="./results/$1"
    echo "Test Disk on $1, the datasets are stored in $src, #init_key is $2, #ops is $3"
    
    bash scripts/prepare_ycsb.sh $src $2 $3 $resfile
}

function run(){
    src="../../../../$1/SOSD/hybrid"
    if [ ! -d "$src" ]; then
        mkdir $src
    fi
    resfile="./results/$1"
    echo "Test Disk on $1, the datasets are stored in $src"
    
    prefix=1011_main_results
    # bash scripts/execute_update_support.sh $src $resfile $prefix
    # bash scripts/execute_memory_budget.sh $src $resfile $prefix
    # bash scripts/execute_rw_tradeoff.sh $src $resfile $prefix
    # bash scripts/execute_hybrid.sh $src $resfile $prefix
    bash scripts/execute_hybrid_cc_curve.sh $src $resfile $prefix
    bash scripts/execute_hybrid_balance.sh $src $resfile $prefix
    bash scripts/execute_baseline.sh $src $resfile $prefix
}

# diskname=(optane PM9A3)
diskname=(PM9A3)
init=150 # in million
ops=10 # in million, ops+init must be less than 200M

for disk in ${diskname[*]}
do
    # prepareYCSB $disk $init $ops
    run $disk
done