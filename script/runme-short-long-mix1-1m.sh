#!/bin/sh

CXX=clang++-6.0

#make clean
#make CXX=$CXX DEBUG=0 LTO=1 MUTEX_ON_CACHELINE=0 -j

if false; then
  make cmake_clean
  cmake -G Ninja . \
-DCMAKE_CXX_COMPILER=${CXX} \
-DCMAKE_BUILD_TYPE=RelWithDebInfo \
-DMUTEX_ON_CACHELINE=OFF \
-DPARTITION=OFF \
-DLTO=ON
ninja
fi

sleep 1


do_expr() {
  th=96
  nrMu=1m
  period=100
  loop=10
  payload=8
  rmw=0
  nrWr4Long=10
  lm=0
  nrOp=10
  nrWr=2
  sm=5

  for rmw in 0 1;
  do
  #for longTxSize in 100 150 200 250 300 350 400 600 800 1k 2k 3k 4k 6k 8k 10k 20k 30k 40k 60k 80k 100k 200k 300k 400k 600k 800k 1m;

  #for longTxSize in 100 1k 4k 10k 20k 50k 100k 200k 500k 1m;  # for mu 1m. 10 longTxSize.
  ##for longTxSize in 100 1k 10k 40k 100k 200k 300k 400k 600k 800k 1m 2m 3m 4m 6m 8m 10m;  # for mu 10m
  #do
  #  shared_opt="-th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -nrwr-long $nrWr4Long -lm $lm -nrop $nrOp -nrwr $nrWr -sm $sm -payload $payload -rmw $rmw"
  #  ./licc_bench $shared_opt -mode licc-pcc 
  #  ./licc_bench $shared_opt -mode licc-hybrid
  #  ./wait_die_bench $shared_opt 
  #  ./nowait_bench $shared_opt 
  #  ./leis_bench $shared_opt -vector 0
  #done

  #for longTxSize in 100 150 200 250 300 350 400 450 500 600 800 1k 10k 100k 1m;  # for mu 1m. 15 longTxSize.
  ##for longTxSize in 100 300 600 1k 2k 3k 4k 6k 8k 10k 100k 1m 10m;  # for mu 10m
  ##for longTxSize in 100 150 200 250 300 350 400;
  #do
  #  shared_opt="-th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -nrwr-long $nrWr4Long -lm $lm -nrop $nrOp -nrwr $nrWr -sm $sm -payload $payload -rmw $rmw"
  #  ./occ_bench $shared_opt
  #  ./tictoc_bench $shared_opt
  #  ./licc_bench $shared_opt -mode licc-occ 
  #done

  #for longTxSize in 100 400 1k 4k 10k 20k 40k 60k 80k 100k 200k;  # for mu 1m. 11 longTxSize.
  for longTxSize in 1k 2k 3k 6k 8k;  # for mu 1m. 11 longTxSize.
  #for longTxSize in 100 400 1k 4k 10k 40k 100k 200k;  # for mu 10m
  do
    shared_opt="-th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -nrwr-long $nrWr4Long -lm $lm -nrop $nrOp -nrwr $nrWr -sm $sm -payload $payload -rmw $rmw"
    ./leis_bench $shared_opt -vector 1  # CAUSION: too slow with large longTxSize.
  done


  done # rmw
}

do_expr | tee -a short-long-mix-1m.log.20180316b
# 100 period, 10 loop, (16, 17) longTxSize, (3, 5) cc
