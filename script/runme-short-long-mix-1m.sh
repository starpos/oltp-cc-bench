#!/bin/sh

CXX=clang++-5.0

#make clean
#make CXX=$CXX DEBUG=0 LTO=1 MUTEX_ON_CACHELINE=0 -j


do_expr() {
  th=96
  nrMu=1m
  period=300
  loop=10

#for longTxSize in 100 200 300 400 600 800 1k 2k 3k 4k 6k 8k 10k 20k 30k 40k 60k 80k 100k 200k 300k 400k 600k 800k 1m;
for longTxSize in 150 250 350 450 500;
#for longTxSize in 100 150 200 250 300 350 400 600 800 1k 2k 3k 4k 6k 8k 10k 20k 30k 40k 60k 80k 100k 200k 300k 400k 600k 800k 1m;
do
  longTxSize2=$(u2s.py $longTxSize)

  #if [ $longTxSize2 -le $(u2s.py 500) ]; then
  #if [ $longTxSize2 -gt $(u2s.py 500) ]; then
    #period=100
    ./occ_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
    ./tictoc_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
    ./licc_bench -mode licc-occ -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
  #fi

  # leis with vector will take too much long time.
  #if [ $(u2s.py $longTxSize) -le $(u2s.py 100k) ]; then
    #./leis_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -vector 1
  #fi

  #if [ $longTxSize2 -ge $(u2s.py 1k) -o $longTxSize2 -eq 100 ]; then
  #if [ $longTxSize2 -lt $(u2s.py 1k) -a $longTxSize2 -ne 100 ]; then
    #period=100
    #./nowait_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
    #./leis_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -vector 0
    
    #period=100
    #./wait_die_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
    #./licc_bench -mode licc-pcc -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
    #./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
  #fi

  #./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -pqlock 7
  #./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -pqlock 3

  #./tlock_bench -mode trlock -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
  #./tlock_bench -mode trlock-occ -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
  #./tlock_bench -mode trlock-hybrid -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
  #./tlock_bench -mode trlock-hybrid -th $th -mu $nrMu -p $period -loop $loop -pqlock 4 -long-tx-size $longTxSize
done

for longTxSize in 60k 80k 100k 200k 300k 400k 600k 800k 1m;
do
    ./licc_bench -mode licc-pcc -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
    ./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
done

for longTxSize in 150k 250k 350k;
do
    ./nowait_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
done
}

do_expr | tee -a short-long-mix-1m.log.20171220
