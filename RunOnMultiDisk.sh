
bash scripts/build_benchmark.sh

function run_script(){
    src="../../../../../$1/SOSD/"
    resfile="./results/$1"
    echo "Test Disk on $1, the datasets are stored in $src, #lookup is $2"

    bash scripts/disk_oriented.sh $src $resfile 1000
    # bash scripts/PG_SameItems.sh $src $resfile
    # bash scripts/test_disk.sh $src $resfile
    # bash scripts/PG_Space.sh $src $resfile $2
    # bash scripts/fetch_strategy.sh $src $resfile $2
    # bash scripts/bottleneck.sh $src $resfile $2
    # bash scripts/compression.sh $src $resfile $2
}

# diskname=(optane PM9A3 ssd hdd)

diskname=(PM9A3)
lookup=10000000
lookup_hdd=1000000


for disk in ${diskname[*]}
do
    if [ $disk == "hdd" ];then
        run_script hdd $lookup_hdd
    else
        run_script $disk $lookup
    fi
done

