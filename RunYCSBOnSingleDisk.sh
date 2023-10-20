bash scripts/build_benchmark.sh ON

function run(){
    src="$1/hybrid"
    if [ ! -d "$src" ]; then
        mkdir $src
    fi
    echo "the datasets are stored in $src"
    
    prefix=1011_main_results
    # bash scripts/execute_update_support.sh $src $2 $prefix
    # bash scripts/execute_memory_budget.sh $src $2 $prefix
    # bash scripts/execute_rw_tradeoff.sh $src $2 $prefix
    # bash scripts/execute_hybrid.sh $src $2 $prefix
    bash scripts/execute_hybrid_cc_curve.sh $src $2 $prefix
    bash scripts/execute_hybrid_balance.sh $src $2 $prefix
    bash scripts/execute_baseline.sh $src $2 $prefix
}

path="./datasets/"
resfile="./results"
run $path $resfile