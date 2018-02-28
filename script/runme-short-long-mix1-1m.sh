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
  loop=4
  payload=8
  rmw=0

  #for longTxSize in 100 150 200 250 300 350 400 600 800 1k 2k 3k 4k 6k 8k 10k 20k 30k 40k 60k 80k 100k 200k 300k 400k 600k 800k 1m;
  for longTxSize in 100 150 200 250 300 350 400 450 500 600 800 1k 10k 100k 1m;
  #for longTxSize in 100 150 200 250 300 350 400;
  do
    shared_opt="-th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -payload $payload -rmw $rmw"
    ./occ_bench $shared_opt
    ./tictoc_bench $shared_opt
    ./licc_bench $shared_opt -mode licc-occ 
  done

  for longTxSize in 100 1k 4k 10k 20k 30k 40k 60k 80k 100k 200k 300k 400k 600k 800k 1m;
  do
    shared_opt="-th $th -mu $nrMu -p $period -loop $loop -long-tx-size $longTxSize -payload $payload -rmw $rmw"
    ./licc_bench $shared_opt -mode licc-pcc 
    ./licc_bench $shared_opt -mode licc-hybrid
    ./wait_die_bench $shared_opt 
    ./nowait_bench $shared_opt 
    ./leis_bench $shared_opt -vector 0
  done
}

do_expr | tee -a short-long-mix-1m.log.20180227a
