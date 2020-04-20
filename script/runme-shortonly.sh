#!/bin/sh

set -x

#make clean
#make CXX=clang++-5.0 DEBUG=0 MUTEX_ON_CACHELINE=1 LTO=1 -j

CXX=clang++-10

if false; then
  make cmake_clean
  cmake -G Ninja . \
-DCMAKE_CXX_COMPILER=${CXX} \
-DCMAKE_BUILD_TYPE=RelWithDebInfo \
-DMUTEX_ON_CACHELINE=ON \
-DPARTITION=OFF \
-DLICC2=ON \
-DLTO=ON
ninja
fi

#for th in 1 2 4 8 12 16 18 27 36 45 54 63 72 81 90 99 108 117 126 135 144; do
#for th in 1 2 4 6 8 12 16 20 24 28 32; do
#for th in 1 2 3 4 6 8 10 12 14 16; do
#for th in 1 2 4 8 12 16 20 24 32 40 48 56 64 72 80 88 96; do  # 17
#for th in 1 2 4 8 12 16 20 24 32 40 48 56 64 72 80 88 96; do  # 17
#for th in 1 32 64 96 128 160 192 224 256; do # 9
#for th in 32 64; do
#for th in 1 8 16 24 32 40 48 56 64; do # 9
#for th in 2 4 6 10 12 14 18 20 22; do # 9
for th in 1 2 4 6 8 10 12 14 16 18 20 22 24 32 40 48 56 64; do # 9
#for th in 12 24 48 96; do
#for th in 3 4 8 12 16 20 24 32 40 48 56 64 72 80 88 96; do
#for th in 3; do
#for th in 32; do
for amode in CUSTOM1; do
#for amode in CUSTOM1; do
#for amode in SOCKET1; do
#for amode in CORE; do
for rmw in 0; do
#for rmw in 0; do
#for nrMuPerTh in 1k 2k 3k 4k 5k 6k 7k 8k 9k 10k 12k 14k 16k 18k 20k 40k 60k 80k 100k; do  # 18
#for nrMuPerTh in 1k 2k 4k 8k 16k; do
for nrMuPerTh in 1k; do
#for nrMu in 1k 3k 10k 30k 100k 300k 1m 3m 10m; do
#for nrMu in 300k; do
#for wrRatio in 0.5 0.05 0; do # 3
for wrRatio in 0.05 0; do # 3
#for wrRatio in 0.5; do # 3
#for wrRatio in 0.5 0.2; do # 3
for backoff in 0; do
  #nrMuPerTh=1000
  workload=custom
  period=10
  loop=10
  payload=8
  nrOp=10
  sm=5

  shared_opt="-th $th -mupt $nrMuPerTh -nrop $nrOp -wrratio $wrRatio -p $period -loop $loop -amode $amode -rmw $rmw -payload $payload -sm $sm"
  #shared_opt="-th $th -mu $nrMu -nrop $nrOp -wrratio $wrRatio -p $period -loop $loop -amode $amode -rmw $rmw -payload $payload -sm $sm"
  #numactl="numactl --localalloc"
  numactl="numactl --interleave=all"
  backoff_opt="-backoff ${backoff}"

  $numactl ./nowait_bench $shared_opt $backoff_opt
  $numactl ./leis_bench $shared_opt -vector 0
  $numactl ./leis_bench $shared_opt -vector 1
  $numactl ./occ_bench $shared_opt $backoff_opt
  $numactl ./tictoc_bench $shared_opt $backoff_opt
  $numactl ./wait_die_bench $shared_opt $backoff_opt
  $numactl ./licc_bench $shared_opt $backoff_opt -mode licc-pcc
  $numactl ./licc_bench $shared_opt $backoff_opt -mode licc-occ
  $numactl ./licc_bench $shared_opt $backoff_opt -mode licc-hybrid -pqlock 0
  $numactl ./licc_bench $shared_opt $backoff_opt -mode licc-hybrid -pqlock 8
#
done
done
done
done
done
done | tee -a short-only.log.20200417a

# 10 cc * 10 period * 10 loop * 1 rmw * 1 amode * 3 wrratio * 9 threads * 1 nrMuPerTh =  27,000 seconds.
# 10 cc * 10 period * 10 loop * 1 rmw * 1 amode * 3 wrratio * 2 threads * 9 nrMu = 54,000 seconds

date
