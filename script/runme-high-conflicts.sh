#!/bin/sh

make clean
make CXX=clang++ DEBUG=0 MUTEX_ON_CACHELINE=1 -j

do_expr()
{
for nrWr in 0 1 2 3 4 5 6 7 8 9 10; do
  for th in 32; do
    nrMu=50
    nrOp=10
    period=10
    loop=10
  
    #./nowait_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5
    #./leis_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -vector 0
    #./leis_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5 -vector 1
    #./occ_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5
    #./tictoc_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5
    #./wait_die_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5
    ./tlock_bench -mode trlock -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5
    ./tlock_bench -mode trlock-occ -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5
    ./tlock_bench -mode trlock-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm 5
    ./tlock_bench -mode trlock-hybrid -th $th -mu $nrMu -p $period -loop $loop -pqlock 4 -nrop $nrOp -nrwr $nrWr -sm 5
  done
done
}
 
do_expr | tee -a high-conflicts.log
