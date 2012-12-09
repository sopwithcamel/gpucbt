if [ $# -eq 0 ]
then num_client=1
else num_client=$1
fi

u=10000000
l=7
for i in `seq 1 $num_client`; do
    time ./service/gpucbtclient 100000000 7 2 &
done
