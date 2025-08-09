# 206 111 331
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
In the `hash_table_v1_add_entry` function, I locked the hash table's mutex before searching for the hash table entry, then if the entry already exists in the hash table, I modified the value then released the lock. If the entry did not exist, I created a new linked list entry, added it to the bucket, then released the lock. The usage of the mutex here verifies that at any given time, only one thread is modifying the hash table.

If the mutex was not locked and unlocked as such, there could be a race condition: when two threads both identify that an entry is not present at similar times, they then two entries and add two items to the linked list, which is incorrect. by using the mutex lock, we ensure that only one thread can identify if the entry is present/not, modify the has table, then release the mutex for the next thread to modify as it needs to do so.


### Performance
```shell
./hash-table-tester -t 4 -s 50000
Generation: 44,060 usec
Hash table base: 217,200 usec
  - 0 missing
Hash table v1: 655,978 usec
  - 0 missing
```
Version 1 is about 302% slower than the base version. This is caused by the single mutex lock for the entire hash table, creating a restriction that only one thread can modify the hash table at a time. If we were to add several entries at once, as does the hash-table-tester program, this essentially equates to almost a sequential single-thread execution time, along with additional overhead from lock management (mutex locking and unlocking), causing significant slowdown in V1 compared to the base implementation.

## Second Implementation
In the `hash_table_v2_add_entry` function, I utilized the design of one mutex for each "bucket" or linked list/hash. The add_entry function performs by calculating the hash for the entry, locking the linked list for the hash, modifying the existing hash table + linked list entry or creating a new linked list entry, then releasing the lock. This avoids race conditions where two threads intend to access the same hash, identify the entry does not exist, and create two duplicate key entries. By locking the mutex at identification of the hash/linked list, we ensure that only one thread searches for a table hit/miss, and performs its modifications, before unlocking and allowing the next thread to complete its modifications.

### Performance
```shell
./hash-table-tester -t 4 -s 50000
Generation: 44,060 usec
Hash table base: 217,200 usec
  - 0 missing
Hash table v1: 655,978 usec
  - 0 missing
Hash table v2: 65,825 usec
  - 0 missing
```

Version 2 is about 330% faster than the base implementation. This speedup is attributed to the mutex-per-bucket design. By creating multiple mutexes, we allow for instances where threads modify distinct linked lists/bucket to occur simultaneously, allowing for speedup, and only restrict the instances where multiple threads access the same linked list/bucket. Since there are 4096 buckets in this implementation, this allows for several oppurtunities where threads do not access the same linked lists/buckets, allowing for greater simultaneous/parallel execution and faster overall runtime.

## Cleaning up
```shell
make clean
```