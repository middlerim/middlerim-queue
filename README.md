### Writer
```sh
RUST_BACKTRACE=1 cargo run --bin writer -- --config middlerim.toml
shmem info:
        Created : true
        link : "./middlerim-queue-index.link"
        os_id : "/shmem_rs_5A24E4326C0A71B7"
        MetaSize : 256
        Size : 1160
        Num locks : 1
        Num Events : 0
        MetaAddr : 0x103f4e000
        UserAddr : 0x103f4e100
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
```

### Reader
```sh
RUST_BACKTRACE=1 cargo run --bin reader -- --config middlerim.toml
shmem info:
        Created : false
        link : "./middlerim-queue-index.link"
        os_id : "/shmem_rs_5A24E4326C0A71B7"
        MetaSize : 256
        Size : 1160
        Num locks : 1
        Num Events : 0
        MetaAddr : 0x108351000
        UserAddr : 0x108351100
IOPS : 13121836, time: 3.589101265s
```
