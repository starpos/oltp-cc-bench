#!/bin/sh

#make clean
#make CXX=clang++-5.0 DEBUG=0 MUTEX_ON_CACHELINE=1 LTO=1 -j
#make CXX=clang++-5.0 DEBUG=0 MUTEX_ON_CACHELINE=0 LTO=1 -j

if false; then
  make cmake_clean
  cmake -G Ninja . \
-DCMAKE_CXX_COMPILER=clang++-5.0 \
-DCMAKE_BUILD_TYPE=RelWithDebInfo \
-DMUTEX_ON_CACHELINE=ON \
-DLTO=ON
ninja
fi

#for th in 1 2 4 8 12 16 18 27 36 45 54 63 72 81 90 99 108 117 126 135 144; do
#for th in 1 2 3 4 6 8 10 12 14 16 20 24 28 32; do
#for th in 1 2 3 4 6 8 10 12 14 16; do
#for th in 1 2 3 4 8 12 16 20 24 32 40 48 56 64 72 80 88 96; do
#for th in 3 4 8 12 16 20 24 32 40 48 56 64 72 80 88 96; do
#for th in 3; do
#for th in 32; do
#for amode in CUSTOM1 CORE; do
for amode in CORE; do
for rmw in 0; do
#for nrWr in 0 1 5 10; do
for nrWr in 0 1 2 3 4 5 6 7 8 9 10; do
for payload in 0 8 16 32 64 128 256 512 1024; do
#for amode in CUSTOM1; do
  th=96
  #th=32
  #nrMuPerTh=4000
  nrMu=100k
  nrOp=10
  workload=custom
  period=10
  loop=10
  #amode=CORE
  #amode=CUSTOM1
  sm=5

  shared_op="-th $th -mu $nrMu -w $workload -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -amode $amode -rmw $rmw -sm $sm -payload $payload"

  ./nowait_bench $shared_op
  ./leis_bench $shared_op -vector 0
  ./leis_bench $shared_op -vector 1
  ./occ_bench $shared_op
  ./tictoc_bench $shared_op 
  ./wait_die_bench $shared_op 
  ./licc_bench $shared_op -mode licc-pcc
  ./licc_bench $shared_op -mode licc-occ
  ./licc_bench $shared_op -mode licc-hybrid -pqlock 0
  ./licc_bench $shared_op -mode licc-hybrid -pqlock 7

done
done
done
done | tee -a short-only-payload.log.20180222b

