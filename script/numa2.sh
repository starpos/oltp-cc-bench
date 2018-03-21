#!/bin/sh

CXX=clang++-6.0
                                      
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

mupt=2000
period=10
loop=7
sm=5
payload=8
rmw=0
amode=CORE

shared_opt="-mupt $mupt -nrop 10 -p $period -loop $loop -amode $amode -rmw $rmw -payload $payload -sm $sm"

for interleave in 0-3 all; do
for workload in custom; do
for nrWr in 0 2 10; do
for th in 1 3 6 12 18 24 30 36 42 48 54 60 66 72 78 84 90 96; do
    numactl --interleave=${interleave} ./occ_bench -th $th -nrwr $nrWr -w $workload $shared_opt \
| tee -a numa.log.20180316a.interleave-${interleave}
done
done
done
done 

# 10 period, 10 loop, 1 workload, 3 nrwr, 18 th
