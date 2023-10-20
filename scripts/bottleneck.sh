echo "Test Bottleneck: CPU time & IO time"
dataset=(fb_200M_uint64 books_200M_uint64 wiki_ts_200M_uint64 osm_cellids_200M_uint64)
fs=4096
lookups=$3
ps=4
bytes=8
fetch_one_page=(0)
total_range=(16 256 512 768 1024)
lambda=(1.05 2 3 4 5)
suffix="_files"
date="bottleneck_"
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
        first=1
        datasrc=$1$data
        storesrc=$1$data$suffix/$fs/$ps/
        for fg in ${fetch_one_page[*]}
        do
            if [ ! -d "$2/bottleneck/" ]; then
                mkdir $2/bottleneck/
            fi
            resfilename="$2/bottleneck/res_${date}_fetch${fg}_${data}.csv"
            echo "start to test dataset on disk: $data, #lookup keys: $lookups, payload bytes: $bytes"
            
            for range in ${total_range[*]}
            do
                echo "range_items: $range"
                para=`expr $range / 2`
                
                # # Run RS
                ./build/LID 1 $datasrc $bytes 1 $lookups RadixSpline $para $first $storesrc $fs $comp $ps $fg >> $resfilename
                echo "RS param: $para, range: $range"
                first=0
                
                # Run PGM
                pgm=`expr $para - 1`
                ./build/LID 1 $datasrc $bytes 1 $lookups PGM-Index-Page $pgm $first $storesrc $fs $comp $ps $fg >> $resfilename
                echo "PGM param: $pgm, pred_gran: 1, range_items: $range"
                first=0
                
                # Run Compressed PGM
                ./build/LID 1 $datasrc $bytes 1 $lookups CompressedPGM $pgm $first $storesrc $fs $comp $ps $fg >> $resfilename
                echo "CompressedPGM param: $pgm, pred_gran: 1, range_items: $range"
                first=0
            done
        done
    fi
done