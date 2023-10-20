echo "Test Disk:"
data=fb_200M_uint64
fs=4096
lookups=10000000
ps=4
bytes=0
suffix="_files"
first=1
comp=0
fetch_one_page=(0 1)
diff=(0 2 4 6 8 10 12 14 16 18 20 22 24 26 28 30 32 34 36) # for 10 pages
date=0326_1threads

if [ ! -f "$1$data" ];then
    echo "$data not exits"
else
    if [ ! -d "$1$data$suffix/" ]; then
        mkdir $1$data$suffix/
    fi
    if [ ! -d "$1$data$suffix/$fs/" ]; then
        mkdir $1$data$suffix/$fs/
    fi
    if [ ! -d "$1$data$suffix/$fs/$ps/" ]; then
        mkdir $1$data$suffix/$fs/$ps/
    fi
    if [ ! -d "$2/testDisk/" ]; then
        mkdir $2/testDisk/
    fi
    datasrc=$1$data
    storesrc=$1$data$suffix/$fs/$ps/
    resfilename="$2/testDisk/res_${date}_10M.csv"
    echo "results are stored in $resfilename"
    for di in ${diff[*]}
    do
        param=`expr $di \* 128`
        for fg in ${fetch_one_page[*]}
        do
            echo "start to test disk: $data, fetch_strategy: $fg, diff: $param, file size: $fs MB, page size: $ps KB, #lookup keys: $lookups, payload bytes: $bytes"
            ./build/LID 1 $datasrc $bytes 1 $lookups DISK $param $first $storesrc $fs $comp $ps $fg >> $resfilename
            echo "------------------------------------------------------"
        done
    done
fi