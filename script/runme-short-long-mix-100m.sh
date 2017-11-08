#!/bin/sh

CXX=clang++-5.0

#make clean
#make CXX=$CXX DEBUG=0 LTO=1 MUTEX_ON_CACHELINE=0 -j


do_expr() {
#for longTxSize in 100 200 300 400 600 800 1k 2k 3k 4k 6k 8k 10k 20k 30k 40k 60k 80k 100k 200k 300k 400k 600k 800k 1m 2m 3m 4m 6m 8m 10m 20m 30m 40m 60m 80m 100m; do
#for longTxSize in 10k 20k 30k 40k 60k 80k 100k 200k 300k 400k 600k 800k 1m 2m 3m 4m 6m 8m 10m 20m 30m 40m 60m 80m 100m; do
for longTxSize in 10m 20m 30m 40m 60m 80m 100m; do
#for longTxSize in 100 300 1k 3k 10k 30k 100k 300k 1m 3m 10m 30m 100m; do
  th=96
  nrMu=100m
  period=1000
  loop=5

  longTxSize2=$(u2s.py $longTxSize)

  #if [ $longTxSize2 -le $(u2s.py 100k) ]; then
    #./occ_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
    #./tictoc_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
    #./licc_bench -mode licc-occ -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
  #fi

  #if [ $(u2s.py $longTxSize) -le $(u2s.py 100k) ]; then
    #./leis_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -vector 1
  #fi

  #if [ $longTxSize2 -eq 100 -o $longTxSize2 -ge $(u2s.py 1k) ]; then
    ./nowait_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
    ./leis_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -vector 0
    ./wait_die_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
    #./licc_bench -mode licc-pcc -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
    ./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
  #fi
  
  #./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -pqlock 7
  #./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -pqlock 3

  #./tlock_bench -mode trlock -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
  #./tlock_bench -mode trlock-occ -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
  #./tlock_bench -mode trlock-hybrid -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
  #./tlock_bench -mode trlock-hybrid -th $th -mu $nrMu -p $period -loop $loop -pqlock 4 -long-tx-size $longTxSize
done
}

do_expr | tee -a short-long-mix-100m.log
