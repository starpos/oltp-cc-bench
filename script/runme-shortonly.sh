#!/bin/sh

set -x

#make clean
#make CXX=clang++-5.0 DEBUG=0 MUTEX_ON_CACHELINE=1 LTO=1 -j

CXX=clang++-6.0

if true; then
  make cmake_clean
  cmake -G Ninja . \
-DCMAKE_CXX_COMPILER=${CXX} \
-DCMAKE_BUILD_TYPE=RelWithDebInfo \
-DMUTEX_ON_CACHELINE=ON \
-DPARTITION=OFF \
-DLTO=ON
ninja
fi

#for th in 1 2 4 8 12 16 18 27 36 45 54 63 72 81 90 99 108 117 126 135 144; do
#for th in 1 2 4 6 8 12 16 20 24 28 32; do
#for th in 1 2 3 4 6 8 10 12 14 16; do
for th in 1 2 4 8 12 16 20 24 32 40 48 56 64 72 80 88 96; do  # 17
#for th in 12 24 48 96; do
#for th in 3 4 8 12 16 20 24 32 40 48 56 64 72 80 88 96; do
#for th in 3; do
#for th in 32; do
#for amode in CUSTOM1 CORE; do
for amode in CUSTOM1; do
#for amode in SOCKET1; do
#for amode in CORE; do
#for rmw in 0 1; do
for rmw in 0; do
#for amode in CUSTOM1; do
#for nrMuPerTh in 1k 2k 3k 4k 5k 6k 7k 8k 9k 10k 12k 14k 16k 18k 20k 40k 60k 80k 100k; do  # 18
for nrMuPerTh in 2k; do
for nrWr in 2; do
  #nrMuPerTh=1000
  workload=custom
  period=10
  loop=10
  payload=8
  nrOp=10
  #nrWr=2
  sm=5

  shared_opt="-th $th -mupt $nrMuPerTh -nrop $nrOp -nrwr $nrWr -p $period -loop $loop -amode $amode -rmw $rmw -payload $payload -sm $sm"
  numactl="numactl --localalloc"

  $numactl ./nowait_bench $shared_opt
  $numactl ./leis_bench $shared_opt -vector 0
  $numactl ./leis_bench $shared_opt -vector 1
  #numactl --localalloc ./occ_bench $shared_opt
  $numactl ./occ_bench $shared_opt
  $numactl ./tictoc_bench $shared_opt
  $numactl ./wait_die_bench $shared_opt
  $numactl ./licc_bench $shared_opt -mode licc-pcc 
  $numactl ./licc_bench $shared_opt -mode licc-occ 
  $numactl ./licc_bench $shared_opt -mode licc-hybrid -pqlock 0
  $numactl ./licc_bench $shared_opt -mode licc-hybrid -pqlock 7

done
done
done
done
done | tee -a short-only.log.20180606b

# 10 cc * 10 period * 10 loop * 1 rmw * 1 amode * 17 threads
