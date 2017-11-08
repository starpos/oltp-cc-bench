#!/bin/sh

CXX=clang++-5.0

#make clean
#make CXX=$CXX DEBUG=0 MUTEX_ON_CACHELINE=1 LTO=1 -j

do_expr()
{
for nrWr in 0 1 2 3 4 5 6 7 8 9 10; do
  for th in 96; do
    for sm in 6 7; do
    nrMu=50
    nrOp=10
    period=10
    #loop=10
    loop=10
  
    #./nowait_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm
    #./leis_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm -vector 0
    #./leis_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm -vector 1
    ./leis_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm -vector 0 -lock 1
    ./leis_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm -vector 1 -lock 1
    #./occ_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm
    #./tictoc_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm
    #./wait_die_bench -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm
    #./tlock_bench -mode trlock -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm
    #./tlock_bench -mode trlock-occ -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm
    #./tlock_bench -mode trlock-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm
    #./tlock_bench -mode trlock-hybrid -th $th -mu $nrMu -p $period -loop $loop -pqlock 4 -nrop $nrOp -nrwr $nrWr -sm $sm
    #./licc_bench -mode licc-pcc -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm
    #./licc_bench -mode licc-occ -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm
    #./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm
    #./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm -pqlock 7
    #./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm -pqlock 1
    #./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm -pqlock 4
    ./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm -pqlock 3
    ./licc_bench -mode licc-hybrid -th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm -pqlock 5
    done
  done
done
}
 
do_expr | tee -a high-conflicts2.log
