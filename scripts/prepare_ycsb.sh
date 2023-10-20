printf "Prepare workloads\n"
init=$2 # in million
ops=$3 # in million

suffix="hybrid"
dataset=(fb_200M_uint64 books_200M_uint64 osm_cellids_200M_uint64)
workloads=(aa cc c)
patterns=(uniform_150)

for data in ${dataset[*]}
do
    for wl in ${workloads[*]}
    do
        for pattern in ${patterns[*]}
        do
            if [ ! -f "$1$data" ];then
                echo "$data not exits"
            else
                if [ ! -d "$1$suffix/" ]; then
                    mkdir $1$suffix/
                fi
                if [ ! -d "$1$suffix/$pattern/" ]; then
                    mkdir $1$suffix/$pattern/
                fi
                if [ ! -d "$1$suffix/$pattern/$data/" ]; then
                    mkdir $1$suffix/$pattern/$data/
                fi
                if [ ! -d "$4/prepare/" ]; then
                    mkdir $4/prepare/
                fi
                save="$1$suffix/${pattern}/${data}/$wl"
                printf "\nprepare $data dataset for workload $wl, #ops is $ops M, #init is $init M, save wordloads on $save, pattern: $pattern\n"
                resfilename="$4/prepare/I${init}M_OPS${ops}M.csv"
                ycsbpath="./ycsb_workloads/${pattern}/txn_monoint_workload$wl"
                ./build/prepare_ycsb $1$data $save $ycsbpath $init $ops >> $resfilename
            fi
        done
    done
done