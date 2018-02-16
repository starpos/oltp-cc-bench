#!/bin/sh

CXX=clang++-5.0

#make clean
#make CXX=$CXX DEBUG=0 MUTEX_ON_CACHELINE=1 LTO=1 -j

make cmake_clean
cmake -G Ninja . \
-DCMAKE_CXX_COMPILER=clang++-5.0 \
-DCMAKE_BUILD_TYPE=RelWithDebInfo \
-DMUTEX_ON_CACHELINE=OFF \
-DLTO=ON
ninja

do_expr()
{
#for nrWr in 0 1 2 3 4 5 6 7 8 9 10; do
#for nrWr in 1 5 10; do
for nrWr in 0 1 5 10; do
  for th in 96; do
    for backoff in 0 1; do
      #for nrMu in 100 1000; do
      for nrMu in 100; do
        for payload in 0 8 16 32 64 128 256 512 1024; do
          #for payload in 8; do
          # nrMu=50
          nrOp=10
          period=10
          loop=10
          #payload=8
          #backoff=1
          rmw=0

          ./nowait_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -backoff $backoff -payload $payload -rmw $rmw
          ./leis_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -vector 0 -lock 0 -payload $payload -rmw $rmw
          ./leis_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -vector 1 -lock 0 -payload $payload -rmw $rmw
          ./occ_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -backoff $backoff -payload $payload -rmw $rmw
          ./tictoc_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -backoff $backoff -payload $payload -rmw $rmw
          ./wait_die_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -backoff $backoff -payload $payload -rmw $rmw
          ./licc_bench -mode licc-pcc    -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -pqlock 0 -backoff $backoff -payload $payload -rmw $rmw
          ./licc_bench -mode licc-occ    -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -pqlock 0 -backoff $backoff -payload $payload -rmw $rmw
          ./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -pqlock 0 -backoff $backoff -payload $payload -rmw $rmw
          ./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -pqlock 7 -backoff $backoff -payload $payload -rmw $rmw
        done
      done
    done
  done
done
}
 
do_expr | tee -a high-conflicts.log.20180209b
