#!/bin/sh

make clean
make CXX=clang++-5.0 DEBUG=0 MUTEX_ON_CACHELINE=1 LTO=1 -j

#for th in 1 2 4 8 12 16 18 27 36 45 54 63 72 81 90 99 108 117 126 135 144; do
#for th in 1 2 3 4 6 8 10 12 14 16 20 24 28 32; do
#for th in 1 2 3 4 6 8 10 12 14 16; do
#for th in 1 2 3 4 8 12 16 20 24 32 40 48 56 64 72 80 88 96; do
#for th in 3 4 8 12 16 20 24 32 40 48 56 64 72 80 88 96; do
#for th in 3; do
#for th in 32; do
#for amode in CUSTOM1 CORE; do
for payload in 0 8 16 32 64 128 256 512 1024; do
for amode in CORE; do
#for amode in CUSTOM1; do
  th=96
  nrMuPerTh=4000
  workload=custom
  period=10
  loop=10
  #amode=CORE
  #amode=CUSTOM1

  #./nowait_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -amode $amode
  #./leis_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -vector 0 -amode $amode
  #./leis_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -vector 1 -amode $amode
  #for rmw in 0 1; do
    #for sm in 0 1; do
      #./occ_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -amode $amode -rmw $rmw -sm $sm
    #done
  #done
  #./tictoc_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -amode $amode
  #./wait_die_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -amode $amode
  for rmw in 1; do
    for sm in 0; do
      ./licc_bench -mode licc-pcc -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -amode $amode -rmw $rmw -sm $sm -payload $payload
      ./licc_bench -mode licc-occ -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -amode $amode -rmw $rmw -sm $sm -payload $payload
      ./licc_bench -mode licc-hybrid -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -pqlock 0 -amode $amode -rmw $rmw -sm $sm -payload $payload
      ./licc_bench -mode licc-hybrid -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -pqlock 7 -amode $amode -rmw $rmw -sm $sm -payload $payload
    done
  done

  #./tlock_bench -mode trlock -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  #./tlock_bench -mode trlock-occ -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  #./tlock_bench -mode trlock-hybrid -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  #./tlock_bench -mode trlock-hybrid -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -pqlock 4
done
done | tee -a short-only-payload.log.20180117a
