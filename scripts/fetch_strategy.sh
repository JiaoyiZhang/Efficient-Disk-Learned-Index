echo "Test Four Fetching Strategies: average IO & throughput"
dataset=(fb_200M_uint64)
fs=4096
lookups=$3
ps=4
payloadbytes=(8)
fetch_strategy=(1 0)
total_range=(16 128 256 512 1024 2048)
suffix="_files"
date=0829_1thread
first=1
comp=0

for bytes in ${payloadbytes[*]}
do
    record=`expr 8 + $bytes`
    item_per_page=`expr $ps \* 1024 / $record`
    echo "items/page: $item_per_page"
    for data in ${dataset[*]}
    do
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
            if [ ! -d "$2/fetchStrategy/" ]; then
                mkdir $2/fetchStrategy/
            fi
            datasrc=$1$data
            storesrc=$1$data$suffix/$fs/$ps/
            for range in ${total_range[*]}
            do
                resfilename="$2/fetchStrategy/res_${date}_${data}.csv"
                first=1
                for fg in ${fetch_strategy[*]}
                do
                    echo "start to test dataset on disk: $data, #lookup keys: $lookups, payload bytes: $bytes, range_items: $range, fetch strategy: $fg"
                    
                    # Run RS
                    para=`expr $range / 2`
                    ./build/LID 1 $datasrc $bytes 1 $lookups RadixSpline $para $first $storesrc $fs $comp $ps $fg >> $resfilename
                    echo "RS param: $para, range: $range"
                    first=0
                    
                    # Run PGM
                    pgm=`expr $para - 1`
                    ./build/LID 1 $datasrc $bytes 1 $lookups PGM-Index-Page $pgm $first $storesrc $fs $comp $ps $fg >> $resfilename
                    echo "PGM param: $pgm, pred_gran: 1, range_items: $range"
                    first=0
                done
            done
        fi
    done
done