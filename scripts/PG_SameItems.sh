echo "Test Prediction Granularity with Same Items: average IO & space cost"
dataset=(books_200M_uint64 osm_cellids_200M_uint64)
fs=4096
lookups=10000000
ps=4
bytes=8
fetch_one_page=(0)
total_range=(256 512 1024)
pred_gran=(1 16 32 64 128 256)
suffix="_files"
date=0911_1thread
first=1
comp=0

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
        if [ ! -d "$2/predictionGran/" ]; then
            mkdir $2/predictionGran/
        fi
        first=1
        for fg in ${fetch_one_page[*]}
        do
            datasrc=$1$data
            storesrc=$1$data$suffix/$fs/$ps/
            resfilename="$2/predictionGran/res_${date}_SameItem_${bytes}B_fetch${fg}_${data}.csv"
            for range in ${total_range[*]}
            do
                for pg in ${pred_gran[*]}
                do
                    para=`expr $range / 2 / $pg`
                    # Run RS
                    if [ $para -gt 0 ]; then
                        actual=`expr 2 \* $para \* $pg - $pg + $item_per_page`
                        echo "start to test dataset on disk: $data, #lookup keys: $lookups, payload bytes: $bytes, pred_gran: $pg, para: $para, range_items: $range, actual_page_items: $actual"
                        
                        ./build/LID 1 $datasrc $bytes $pg $lookups RadixSpline $para $first $storesrc $fs $comp $ps $fg >> $resfilename
                        first=0
                    fi
                done
            done
        done
    fi
done