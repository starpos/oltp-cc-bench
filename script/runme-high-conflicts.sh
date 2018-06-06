#!/bin/sh

CXX=clang++-6.0

#make clean
#make CXX=$CXX DEBUG=0 MUTEX_ON_CACHELINE=1 LTO=1 -j

if true; then
  make cmake_clean
  cmake -G Ninja . \
-DCMAKE_CXX_COMPILER=${CXX} \
-DCMAKE_BUILD_TYPE=RelWithDebInfo \
-DMUTEX_ON_CACHELINE=ON \
-DPARTITION=OFF \
-DLTO=ON
ninja
fi

do_expr()
{
for backoff in 0 1; do
  for rmw in 0 1; do
    for nrWr in 0 1 2 3 4 5 6 7 8 9 10; do
    #for nrWr in 0 1 5 10; do
      #for nrMu in 100 1000; do
      for nrMu in 100; do
        for th in 96; do
          #for payload in 0 8 16 32 64 128 256 512 1024; do
          for payload in 8; do
            nrOp=10
            period=10
            loop=10
            sm=5
  
            shared_opt="-th $th -mu $nrMu -p $period -loop $loop -nrop $nrOp -nrwr $nrWr -sm $sm -payload $payload -rmw $rmw"

            ./nowait_bench $shared_opt -backoff $backoff
            ./leis_bench $shared_opt -vector 0 -lock 0 
            ./leis_bench $shared_opt -vector 1 -lock 0 
            ./occ_bench $shared_opt -backoff $backoff
            ./tictoc_bench $shared_opt -backoff $backoff
            ./wait_die_bench $shared_opt -backoff $backoff
            ./licc_bench $shared_opt -mode licc-pcc    -pqlock 0 -backoff $backoff
            ./licc_bench $shared_opt -mode licc-occ    -pqlock 0 -backoff $backoff
            ./licc_bench $shared_opt -mode licc-hybrid -pqlock 0 -backoff $backoff
            ./licc_bench $shared_opt -mode licc-hybrid -pqlock 7 -backoff $backoff
          done
        done
      done
    done
  done
done
}
 
do_expr | tee -a high-conflicts.log.20180606a
