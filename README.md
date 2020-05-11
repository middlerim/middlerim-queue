## Benchmark
Run both the reader and the writer simultaneously.

### Reader
```sh
$ cargo run --bin reader_bench --release
shmem info:
        Created : false
        link : "./data/middlerim-queue"
        os_id : "/shmem_rs_4616C511769BABEE"
        MetaSize : 57600
        Size : 138412064
        Num locks : 257
        Num Events : 0
        MetaAddr : 0x109724000
        UserAddr : 0x109732100
27000000, 8, "29927551"
end: 6081

188K messages read/s. Total time: 143.95143023s
```

### Writer
```sh
$ cargo run --bin writer_bench --release
shmem info:
        Created : false
        link : "./data/middlerim-queue"
        os_id : "/shmem_rs_4616C511769BABEE"
        MetaSize : 57600
        Size : 138412064
        Num locks : 257
        Num Events : 0
        MetaAddr : 0x104fdf000
        UserAddr : 0x104fed100
6081, 300000000
207K messages write/s. Total time: 144.397172045s
```

## Java
Call writer and reader from Java.
This example call them one by one, not simultaneously.

```sh
$ cd jni
$ ./build.sh
1428K messages write&read/s. Total time: 7s
```
