#!/bin/sh

CXX=clang++-5.0

#make clean
#make CXX=$CXX DEBUG=0 MUTEX_ON_CACHELINE=1 LTO=1 -j

do_expr()
{
#for nrWr in 0 1 2 3 4 5 6 7 8 9 10; do
for nrWr in 8 9 10; do
  for th in 96; do
    for backoff in 0 1; do
      for nrMu in 100 1000 1000; do
    # nrMu=50
    nrOp=10
    period=10
    loop=10
    #backoff=1
  
    ./nowait_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -backoff $backoff
    ./leis_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -vector 0 -lock 0
    ./leis_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -vector 1 -lock 0
    ./occ_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -backoff $backoff
    ./tictoc_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -backoff $backoff
    ./wait_die_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -backoff $backoff
    ./licc_bench -mode licc-pcc    -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -pqlock 0 -backoff $backoff
    ./licc_bench -mode licc-occ    -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -pqlock 0 -backoff $backoff
    ./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -pqlock 0 -backoff $backoff
    ./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -pqlock 3 -backoff $backoff
    ./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -pqlock 7 -backoff $backoff

    #./wait_die_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -txid-gen 1
    #./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -pqlock 4
    #./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -pqlock 5
    #./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -pqlock 1
    #./leis_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -vector 0 -lock 1
    #./leis_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -vector 1 -lock 1
    #./tlock_bench -mode trlock -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5
    #./tlock_bench -mode trlock-occ -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5
    #./tlock_bench -mode trlock-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5
    #./tlock_bench -mode trlock-hybrid -th $th -mu $nrMu -p $period -loop $loop -pqlock 4 -nrop $nrOp -nrwr $nrWr -sm 5
    #./licc_bench -mode licc-pcc -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -pqlock 0
    #./licc_bench -mode licc-occ -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -pqlock 0
    #./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -qplock 0
    #./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -pqlock 7
    #./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -pqlock 3
    done
    done
  done
done
}
 
do_expr | tee -a high-conflicts.log.20180120a
