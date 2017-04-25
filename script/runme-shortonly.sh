#!/bin/sh

make clean
make CXX=clang++ DEBUG=0 -j

#for th in 1 2 4 8 12 16 18 27 36 45 54 63 72 81 90 99 108 117 126 135 144; do
for th in 1 2 3 4 6 8 10 12 14 16 20 24 28 32; do
#for th in 32; do
  nrMuPerTh=4000
  workload=custom
  period=10
  loop=10

  #./nowait_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  #./leis_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -vector 0
  #./leis_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -vector 1
  #./occ_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  #./tictoc_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  #./wait_die_bench -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  ./tlock_bench -mode trlock -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  ./tlock_bench -mode trlock-occ -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  ./tlock_bench -mode trlock-hybrid -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop
  ./tlock_bench -mode trlock-hybrid -th $th -mupt $nrMuPerTh -w $workload -p $period -loop $loop -pqlock 4
done | tee -a short-only.log

