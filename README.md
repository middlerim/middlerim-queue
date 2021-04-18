## Benchmark
Run both the reader and the writer simultaneously.

### Writer
```sh
$ cargo run --bin writer_bench --release
56251, 500000000
2498K messages write/s. Total time: 20.010147357s
```

### Reader
Run simultaneously with Writer above.

```sh
$ cargo run --bin reader_bench --release
32500000, 8, "49675749"
end: 56251

2139K messages read/s. Total time: 15.341729856s
```

## Java
Call writer and reader from Java.
This example call them one by one, not simultaneously.

```sh
$ cd jni
$ ./run_example.sh
Read message(index=38529):  🐓 🏰 🥕
1250K messages write&read/s. Total time: 8s
```
