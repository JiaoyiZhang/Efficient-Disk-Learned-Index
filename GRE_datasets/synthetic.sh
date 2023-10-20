le=8
ge=512
local_models=(1 2 4) #1e6
global_models=(10 12) #1e4

for lv in ${local_models[*]}
do
    for gv in ${global_models[*]}
    do
        local=`expr $lv \* 1000000`
        global=`expr $gv \* 10000`
        store="../../../../../optane/SOSD/syn_hard_g${gv}_l${lv}"
        ./generator $le $ge $local $global 200000000 $store
        echo "generator 200000000 datasets to: $store"
        echo "-----------------------------------------------"
    done
done