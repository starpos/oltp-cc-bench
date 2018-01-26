#!/bin/sh

#make clean
#make CXX=clang++-5.0 DEBUG=0 MUTEX_ON_CACHELINE=1 LTO=1 -j

make cmake_clean
cmake -G Ninja . \
-DCMAKE_CXX_COMPILER=clang++-5.0 \
-DCMAKE_BUILD_TYPE=RelWithDebInfo \
-DMUTEX_ON_CACHELINE=ON \
-DLTO=ON
ninja

#for th in 1 2 4 8 12 16 18 27 36 45 54 63 72 81 90 99 108 117 126 135 144; do
for th in 1 2 4 6 8 12 16 20 24 28 32; do
#for th in 1 2 3 4 6 8 10 12 14 16; do
#for th in 1 2 3 4 8 12 16 20 24 32 40 48 56 64 72 80 88 96; do
#for th in 3 4 8 12 16 20 24 32 40 48 56 64 72 80 88 96; do
#for th in 3; do
#for th in 32; do
#for amode in CUSTOM1 CORE; do
for amode in CORE; do
#for amode in CUSTOM1; do
  nrMuPerTh=4000
  workload=custom
  period=10
  loop=10
  payload=8
  #amode=CORE
  #amode=CUSTOM1

  ./nowait_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -amode $amode -payload $payload
  ./leis_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -vector 0 -amode $amode -payload $payload
  ./leis_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -vector 1 -amode $amode -payload $payload
  for rmw in 0 1; do
    for sm in 0; do
      ./occ_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -amode $amode -rmw $rmw -sm $sm -payload $payload
    done
  done
  ./tictoc_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -amode $amode -payload $payload
  ./wait_die_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -amode $amode -payload $payload
  for rmw in 0 1; do
    for sm in 0; do
      ./licc_bench -mode licc-pcc -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -amode $amode -rmw $rmw -sm $sm -payload $payload
      ./licc_bench -mode licc-occ -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -amode $amode -rmw $rmw -sm $sm -payload $payload
      ./licc_bench -mode licc-hybrid -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -pqlock 0 -amode $amode -rmw $rmw -sm $sm -payload $payload
      ./licc_bench -mode licc-hybrid -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -pqlock 7 -amode $amode -rmw $rmw -sm $sm -payload $payload
    done
  done

done
done | tee -a short-only.log.20180125b

