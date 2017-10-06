#!/bin/sh

BIN=cas_bench2

for CXX in g++-5.4.0 g++-6.4.0 g++-7.2.0 clang++-4.0 clang++-5.0;
do
    make clean && make CXX=$CXX $BIN
    ./$BIN |ts "compiler:$CXX"
done |tee expr.log
