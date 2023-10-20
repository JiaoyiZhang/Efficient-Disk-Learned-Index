bash scripts/build_benchmark.sh ON

function prepareYCSB(){
    echo "Test Disk, the datasets are stored in $1, #init_key is $2, #ops is $3, results are stored in $4"
    
    bash scripts/prepare_ycsb.sh $1 $2 $3 $4
}


init=150 # in million
ops=10 # in million, ops+init must be less than 200M
src="./datasets/"
resfile="./results"

prepareYCSB $src $init $ops $resfile