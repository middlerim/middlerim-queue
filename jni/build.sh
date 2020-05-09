#!/usr/bin/env bash

javac -h . java/io/middlerim/queue/Writer.java || exit 1
javac -h . java/io/middlerim/queue/Reader.java || exit 1

cargo build --release || exit 1
cp ../target/debug/libmiddlerimq.dylib ./java || exit 1

javac java/io/middlerim/queue/*.java || exit 1

cd java

java -Djava.library.path=. io.middlerim.queue.Example || exit 1
