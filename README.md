# Benchmarks of Concurrency Control Methods for OLTP

## Abstract

This is prototype code to measure concurrency control methods.


## Build

`make CXX=clang++ MUTEX_ON_CACHELINE=1 DEBUG=0 -j`

- Clang-4.0 seems generate faster code than gcc-6.3.
- See `Makefile` for details.

## Usage

See `script/runme-*.sh` or run `*_bench` with `-h` option.

## Plotting Graphs

See `script/*.ipy`.

## License

See LICENSE files.
