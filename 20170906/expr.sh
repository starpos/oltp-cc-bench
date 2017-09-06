#!/bin/sh

BIN=cas_bench2

(
make clean && make CXX=g++ $BIN
./$BIN |ts 'compiler:gcc-5.4'
make clean && make CXX=g++-6.4 $BIN
./$BIN |ts 'compiler:gcc-6.4'
make clean && make CXX=g++-7.2 $BIN
./$BIN |ts 'compiler:gcc-7.2'
make clean && make CXX=clang++ $BIN
./$BIN |ts 'compiler:clang-4.0.1'
) |tee expr.log
