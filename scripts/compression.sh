echo "Test Disk-Oriented Indexes: throughput & space cost"
# # hard datasets
# dataset=(syn_hard_g10_l1 syn_hard_g10_l2 syn_hard_g10_l4 syn_hard_g12_l1 syn_hard_g12_l4)

# # Datasets with Varying Difficulty
# dataset=(syn_curve_512g1_l6 syn_curve_512g2_l6 syn_curve_512g3_l6 syn_curve_512g4_l6 syn_curve_512g5_l6 syn_curve_512g6_l6 syn_curve_512g8_l6 syn_curve_512g10_l6 syn_curve_512g12_l6 syn_curve_512g13_l6 syn_curve_512g15_l6 syn_curve_512g17_l7 syn_curve_512g19_l1)

# SOSD
dataset=(fb_200M_uint64 books_200M_uint64 wiki_ts_200M_uint64 osm_cellids_200M_uint64)

fs=4096
lookups=$3
ps=4
payloadbytes=(8)
fetch_one_page=(0)
total_range=(16 256 512 768 1024)
bs_range=(256 512 768 1024 1280)
lambda=(1.05 2 3 4 5)
suffix="_files"
date=0926_reduce_memory
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
            if [ ! -d "$2/compression/" ]; then
                mkdir $2/compression/
            fi
            first=1
            datasrc=$1$data
            storesrc=$1$data$suffix/$fs/$ps/
            for fg in ${fetch_one_page[*]}
            do
                resfilename="$2/compression/res_${date}_${bytes}B_fetch${fg}_${data}.csv"
                echo "start to test dataset on disk: $data, #lookup keys: $lookups, payload bytes: $bytes"
                
                # Run Disk-based Indexes
                for lb in ${lambda[*]}
                do
                    # Run PGM_Disk
                    echo "start to train disk-based index: PGM_Disk, lambda: $lb"
                    ./build/LID 1 $datasrc $bytes 1 $lookups DI-V1 $lb $first $storesrc $fs $comp $ps $fg >> $resfilename
                    echo "    Evaluation on PGM_Disk has been completed!"
                    first=0
                    
                    # Run Cpr_PGM_Disk
                    echo "start to train disk-based index: Cpr_PGM_Disk, lambda: $lb"
                    ./build/LID 1 $datasrc $bytes 1 $lookups DI-V3 $lb $first $storesrc $fs $comp $ps $fg >> $resfilename
                    echo "    Evaluation on Cpr_PGM_Disk has been completed!"
                    first=0
                    
                    # Run CprLeCo_PGM_Disk
                    echo "start to train disk-based index: CprLeCo_PGM_Disk, lambda: $lb"
                    ./build/LID 1 $datasrc $bytes 1 $lookups DI-V4 $lb $first $storesrc $fs $comp $ps $fg >> $resfilename
                    echo "    Evaluation on CprLeCo_PGM_Disk has been completed!"
                    first=0
                done
                
                for bs in ${bs_range[*]}
                do
                    # Run Zone Map
                    ./build/LID 1 $datasrc $bytes 1 $lookups BinarySearch $bs $first $storesrc $fs $comp $ps $fg >> $resfilename
                    echo "    Evaluation on BinarySearch $bs has been completed!"
                    first=0
                    
                    # Run LeCo-based Zone Map
                    echo "start to train LeCo-based Zone Map:"
                    ./build/LID 1 $datasrc $bytes 1 $lookups LecoZonemap $bs $first $storesrc $fs $comp $ps $fg $data >> $resfilename
                    echo "    Evaluation on LeCo-based Zone Map $bs has been completed!"
                    first=0
                    
                    # Run Leco-Zonemap-Disk
                    echo "start to train Leco-Zonemap-Disk:"
                    ./build/LID 1 $datasrc $bytes 1 $lookups LecoPage $bs $first $storesrc $fs $comp $ps $fg $data >> $resfilename
                    echo "    Evaluation on Leco-Zonemap-Disk $bs has been completed!"
                    first=0
                done
                
                for range in ${total_range[*]}
                do
                    echo "range_items: $range"
                    para=`expr $range / 2`
                    pgm=`expr $para - 1`
                    
                    # Run PGM
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
done