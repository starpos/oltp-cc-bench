#!/bin/sh

CXX=clang++-5.0

#make clean
#make CXX=$CXX DEBUG=0 LTO=1 MUTEX_ON_CACHELINE=0 -j


do_expr() {
  th=96
  nrMu=1m
  period=10
  loop=10
  longTxSize=1k
  lm=9

#for longTxSize in 100 200 300 400 600 800 1k 2k 3k 4k 6k 8k 10k 20k 30k 40k 60k 80k 100k 200k 300k 400k 600k 800k 1m;
for nrTh4LongTx in 1 2 3 4 5 6 7 8 9 10;
#for longTxSize in 100 150 200 250 300 350 400 600 800 1k 2k 3k 4k 6k 8k 10k 20k 30k 40k 60k 80k 100k 200k 300k 400k 600k 800k 1m;
do
  for rmw in 0 1; do
    ./occ_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -th-long $nrTh4LongTx -lm $lm -rmw $rmw
    ./tictoc_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -th-long $nrTh4LongTx -lm $lm -rmw $rmw
    ./licc_bench -mode licc-occ -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -th-long $nrTh4LongTx -lm $lm -rmw $rmw
    ./licc_bench -mode licc-pcc -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -th-long $nrTh4LongTx -lm $lm -rmw $rmw
    ./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -th-long $nrTh4LongTx -pqlock 0 -lm $lm -rmw $rmw
    ./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -th-long $nrTh4LongTx -pqlock 7 -lm $lm -rmw $rmw
  done
  ./nowait_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -th-long $nrTh4LongTx -lm $lm
  ./leis_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -th-long $nrTh4LongTx -vector 0 -lm $lm
  ./nowait_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -th-long $nrTh4LongTx -lm $lm
  ./wait_die_bench -th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -th-long $nrTh4LongTx -lm $lm
done
}

do_expr | tee -a short-long-mix2-1m.log.20180117c
