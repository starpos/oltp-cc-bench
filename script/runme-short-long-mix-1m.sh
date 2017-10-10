#!/bin/sh

CXX=clang++-5.0

make clean
make CXX=$CXX DEBUG=0 MUTEX_ON_CACHELINE=0 -j


do_expr() {
for longTxSize in 100 200 300 400 600 800 1k 2k 3k 4k 6k 8k 10k 20k 30k 40k 60k 80k 100k 200k 400k 600k 800k 1m; do
  th=16
  nrMu=1m
  period=300
  loop=1

  ./nowait_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
  ./leis_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -vector 0
  ./leis_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -vector 1
  ./occ_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
  ./tictoc_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
  ./wait_die_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
  ./tlock_bench -mode trlock -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
  ./tlock_bench -mode trlock-occ -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
  ./tlock_bench -mode trlock-hybrid -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
  ./tlock_bench -mode trlock-hybrid -th $th -mu $nrMu -p $period -loop $loop -pqlock 4 -long-tx-size $longTxSize
  ./licc_bench -mode licc-pcc -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
  ./licc_bench -mode licc-occ -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
  ./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize
done
}

do_expr | tee -a short-long-mix-1m.log
