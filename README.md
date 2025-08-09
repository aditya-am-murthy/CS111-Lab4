# Hash Hash Hash
C implementations of Hash Tables of thread-safe, mutex only in 3 ways: single-threaded, single-mutex multi-threaded, and multi-mutex multi-threaded.

## Building
```shell
make
```

## Running
```shell
./hash-table-tester -t [thread count] -s [entries]
```

## First Implementation
In the `hash_table_v1_add_entry` function, I added TODO

### Performance
```shell
TODO how to run and results
```
Version 1 is a little slower/faster than the base version. As TODO

## Second Implementation
In the `hash_table_v2_add_entry` function, I TODO

### Performance
```shell
TODO how to run and results
```

TODO more results, speedup measurement, and analysis on v2

## Cleaning up
```shell
make clean
```