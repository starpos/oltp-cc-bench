#!/bin/sh

BIN=cas_bench2

(
make clean && make CXX=g++ LTO=1 $BIN
./$BIN |ts 'compiler:gcc-5.4'
make clean && make CXX=g++-6.4 LTO=1 $BIN
./$BIN |ts 'compiler:gcc-6.4'
make clean && make CXX=g++-7.2 LTO=1 $BIN
./$BIN |ts 'compiler:gcc-7.2'
make clean && make CXX=clang++-4.0 LTO=1 $BIN
./$BIN |ts 'compiler:clang-4.0'
make clean && make CXX=clang++-5.0 LTO=1 $BIN
./$BIN |ts 'compiler:clang-5.0'
) |tee expr.log
