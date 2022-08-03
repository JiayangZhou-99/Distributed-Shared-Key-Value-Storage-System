# Distributed-Shared-Key-Value-Storage-System
A distributed key/value storage system with Golang that stay consistent between raft-based replica servers.



- Implemented a simplified version **Raft** consistency protocol, except leader election.
- Replicate **Operation Log** between replica servers to guarantee consistency and achieve **Version Control**. 
- Isolate file meta and file storage and Storing file hash value and file block data in **MongoDB** to avoid duplicate file.
- Applied **Goroutines**, **Channels**, and **RPCs** to enable **Remote**, **Concurrent** **Parallel** communication between servers.

## How to run the code

Run BlockStore server:

```console
$ make run-blockstore
```

Run RaftSurfstore server:

```console
$ make IDX=0 run-raft
```

Test:

```console
$ make test
```

Specific Test:

```console
$ make TEST_REGEX=Test specific-test
```

Clean:

```console
$ make clean
```





