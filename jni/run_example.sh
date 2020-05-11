#!/usr/bin/env bash

cd driver
./gradlew clean build || exit 1

javac -h . src/main/java/io/middlerim/queue/Writer.java || exit 1
javac -h . src/main/java/io/middlerim/queue/Reader.java || exit 1

cd ..

cargo build --release || exit 1
cp ../target/release/libmiddlerimq.dylib driver/build/libs || exit 1
# cargo build || exit 1
# cp ../target/debug/libmiddlerimq.dylib driver/build/libs || exit 1

cp ../middlerim-*.toml driver/build/libs || exit 1


cd driver/build/libs

mkdir data

java -Djava.library.path=. -cp middlerim-queue.jar io.middlerim.queue.Example || exit 1
