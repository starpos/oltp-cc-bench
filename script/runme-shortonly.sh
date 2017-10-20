#!/bin/sh

#make clean
#make CXX=clang++-5.0 DEBUG=0 MUTEX_ON_CACHELINE=1 LTO=1 -j

#for th in 1 2 4 8 12 16 18 27 36 45 54 63 72 81 90 99 108 117 126 135 144; do
#for th in 1 2 3 4 6 8 10 12 14 16 20 24 28 32; do
#for th in 1 2 3 4 6 8 10 12 14 16; do
#for th in 1 2 3 4 8 12 16 20 24 32 40 48 56 64 72 80 88 96; do
for th in 3 4 8 12 16 20 24 32 40 48 56 64 72 80 88 96; do
#for th in 3; do
#for th in 32; do
  nrMuPerTh=4000
  workload=custom
  period=10
  loop=10

  #./nowait_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  #./leis_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -vector 0
  #./leis_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -vector 1
  ./occ_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  #./tictoc_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  #./wait_die_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  #./tlock_bench -mode trlock -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  #./tlock_bench -mode trlock-occ -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  #./tlock_bench -mode trlock-hybrid -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  #./tlock_bench -mode trlock-hybrid -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -pqlock 4
  #./licc_bench -mode licc-pcc -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  #./licc_bench -mode licc-occ -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  #./licc_bench -mode licc-hybrid -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  #./licc_bench -mode licc-hybrid -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -pqlock 7
done | tee -a short-only.log.1

