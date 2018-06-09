#!/bin/sh

set -x


CXX=clang++-6.0

make cmake_clean
cmake -G Ninja . \
-DCMAKE_CXX_COMPILER=${CXX} \
-DCMAKE_BUILD_TYPE=RelWithDebInfo \
-DMUTEX_ON_CACHELINE=ON \
-DPARTITION=OFF \
-DNO_PAYLOAD=OFF \
-DLTO=ON
#ninja

