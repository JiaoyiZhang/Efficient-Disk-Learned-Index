echo "Test Prediction Granularity with Same Items: average IO & space cost"
dataset=(books_200M_uint64 osm_cellids_200M_uint64)
fs=4096
lookups=$2
ps=4
payloadbytes=(8)
total_range=(128 256 512 1024)
pred_gran=(1 16 32 64 128 256)
suffix="_files"
date=0901
first=1

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
            if [ ! -d "$2/predictionGran/" ]; then
                mkdir $2/predictionGran/
            fi
            first=1
            datasrc=$1$data
            resfilename="$2/predictionGran/res_${date}_SameItem_${data}.csv"
            for range in ${total_range[*]}
            do
                for pg in ${pred_gran[*]}
                do
                    para=`expr $range / 2 / $pg`
                    echo "start to test dataset on disk: $data, #lookup keys: $lookups, payload bytes: $bytes, pred_gran: $pg, para: $para, range_items: $range"
                    # Run RS
                    if [ $para -gt 0 ]; then
                        ./build/LID 0 $datasrc $bytes $pg $lookups RadixSpline $para $first >> $resfilename
                        echo "RS param: $para, pred_gran: $pg, range_items: $range"
                        first=0

                        ./build/LID 0 $datasrc $bytes $pg $lookups RS-PG $para $first >> $resfilename
                        echo "RS-PG param: $para, pred_gran: $pg, range_items: $range"
                        first=0
                    fi

                    # Run PGM
                    if [ $para -gt 1 ]; then
                        pgm=`expr $para - 1`
                        ./build/LID 0 $datasrc $bytes $pg $lookups PGM-Index-Page $pgm $first >> $resfilename
                        echo "PGM param: $pgm, pred_gran: $pg, range_items: $range"
                        first=0

                        ./build/LID 0 $datasrc $bytes $pg $lookups PGM-PG $pgm $first >> $resfilename
                        echo "PGM-PG param: $pgm, pred_gran: $pg, range_items: $range"
                    fi
                done
            done
        fi
    done
done