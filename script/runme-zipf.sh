#!/bin/sh

set -x

#make clean
#make CXX=clang++-5.0 DEBUG=0 MUTEX_ON_CACHELINE=1 LTO=1 -j

CXX=clang++-6.0

if false; then
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
#for th in 96; do
#for amode in CUSTOM1 CORE; do
for amode in CUSTOM1; do
#for amode in SOCKET1; do
#for amode in CORE; do
#for rmw in 0 1; do
for rmw in 0; do
#for rmw in 0; do
#for amode in CUSTOM1; do
#for nrMuPerTh in 1k 2k 3k 4k 5k 6k 7k 8k 9k 10k 12k 14k 16k 18k 20k 40k 60k 80k 100k; do  # 18
for nrMu in 10m; do
for nrWr in 1 8; do
for backoff in 0 1; do
#for theta in 0.0 0.4 0.6 0.8 0.9 0.95 0.99; do
for theta in 0.9 0.99; do
#for payload in 8 100; do
for payload in 100; do
  #nrMuPerTh=1000
  workload=custom
  period=10
  loop=10
  #payload=100
  nrOp=16
  #nrWr=2
  sm=5
  #theta=0.99

  shared_opt="-th $th -mu $nrMu -nrop $nrOp -nrwr $nrWr -p $period -loop $loop -amode $amode -rmw $rmw -payload $payload -sm $sm -zipf -theta $theta"
  numactl="numactl --localalloc"

  $numactl ./nowait_bench $shared_opt -backoff $backoff
  if [ $backoff -eq 0 ]; then
    $numactl ./leis_bench $shared_opt -vector 0
    $numactl ./leis_bench $shared_opt -vector 1
  fi
  $numactl ./occ_bench $shared_opt -backoff $backoff
  $numactl ./tictoc_bench $shared_opt -backoff $backoff
  $numactl ./wait_die_bench $shared_opt -backoff $backoff
  $numactl ./licc_bench $shared_opt -backoff $backoff -mode licc-pcc
  $numactl ./licc_bench $shared_opt -backoff $backoff -mode licc-occ
  $numactl ./licc_bench $shared_opt -backoff $backoff -mode licc-hybrid -pqlock 0
  $numactl ./licc_bench $shared_opt -backoff $backoff -mode licc-hybrid -pqlock 7

done  # payload
done  # theta
done  # backoff
done  # nrWr
done  # nrMu
done  # rmw
done  # amode
done | tee -a zipf-th.log.20180627a
#done | tee -a zipf-skew.log.20180618a

# 10 cc * 10 period * 10 loop * 2 backoff * 2 nrwr * 2 rmw * 1 amode * 17 threads
# 10 cc * 10 period * 10 loop * 7 theta * 2 backoff * 2 nrwr * 2 rmw * 1 amode * 1 threads = 56,000sec
# 10 cc * 10 period * 10 loop * 2 payload * 2 theta * 2 backoff * 2 nrwr * 2 rmw * 1 amode * 17 threads = 
