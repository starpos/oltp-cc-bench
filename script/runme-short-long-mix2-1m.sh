#!/bin/sh

CXX=clang++-5.0

#make clean
#make CXX=$CXX DEBUG=0 LTO=1 MUTEX_ON_CACHELINE=0 -j

if true; then
  make cmake_clean
  cmake -G Ninja . \
-DCMAKE_CXX_COMPILER=clang++-5.0 \
-DCMAKE_BUILD_TYPE=RelWithDebInfo \
-DMUTEX_ON_CACHELINE=OFF \
-DLTO=ON
ninja
fi

sleep 1

do_expr() {
  th=96
  nrMu=1m
  period=100
  loop=20
  lm=5
  longTxSize=100k
  nrWr4Long=1k
  sm=5
  nrOp=10
  nrWr=2
  #rmw=1
  #rmw=0
  for rmw in 0 1; do

  #for longTxSize in 100 200 300 400 600 800 1k 2k 3k 4k 6k 8k 10k 20k 30k 40k 60k 80k 100k 200k 300k 400k 600k 800k 1m;
  #for longTxSize in 100 150 200 250 300 350 400 600 800 1k 2k 3k 4k 6k 8k 10k 20k 30k 40k 60k 80k 100k 200k 300k 400k 600k 800k 1m;
  #for nrTh4LongTx in 1 2 3 4 5 6 7 8 9 10; do
  #for nrTh4LongTx in 1 2 3 4 6 8 10 12 14 16 18 20 25 30 35 40 45 50; do
  for nrTh4LongTx in 1 2 3 4 6 8 10 12 14 16 18 20; do
  #for nrTh4LongTx in 19 20; do
  #for nrTh4LongTx in 1 2 3 4 5 7 10 15; do
    shared_opt="-th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -nrwr-long $nrWr4Long -th-long $nrTh4LongTx -lm $lm -sm $sm -nrop $nrOp -nrwr $nrWr -rmw $rmw"
    #./occ_bench $shared_opt
    #./tictoc_bench $shared_opt
    #./licc_bench $shared_opt -mode licc-occ
    #./licc_bench $shared_opt -mode licc-pcc 
    ./licc_bench $shared_opt -mode licc-hybrid -pqlock 0
    #./licc_bench $shared_opt -mode licc-hybrid -pqlock 7
    ./nowait_bench $shared_opt 
    ./leis_bench $shared_opt -vector 0
    ./wait_die_bench $shared_opt
  done
  ## (100 period) * (20 loop) * (4 cc) * (12 nrTh4LongTx)
  done
}

do_expr | tee -a short-long-mix2-1m.log.20180309a

#./licc_bench -th 96 -mu 1m -p 10 -loop 1 -long-tx-size 100k -th-long 10 -lm 0 -rmw 0 -nrop 100 -nrwr 10 -sm 5  -mode licc-hybrid
