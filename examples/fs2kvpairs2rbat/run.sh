#!/bin/bash

export BUCKETS_ROOT_DIR=./sample.d
export MAX_FILE_SIZE=1024

geninput(){
	mkdir -p "${BUCKETS_ROOT_DIR}"

	mkdir -p "${BUCKETS_ROOT_DIR}/bkt0.d"
	mkdir -p "${BUCKETS_ROOT_DIR}/bkt1.d"
	mkdir -p "${BUCKETS_ROOT_DIR}/bkt2.d"

	echo -n dat00 > "${BUCKETS_ROOT_DIR}/bkt0.d/key0.txt"
	echo -n dat01 > "${BUCKETS_ROOT_DIR}/bkt0.d/key1.txt"

	echo -n dat10 > "${BUCKETS_ROOT_DIR}/bkt1.d/key0.txt"
	echo -n dat11 > "${BUCKETS_ROOT_DIR}/bkt1.d/key1.txt"

	echo -n dat20 > "${BUCKETS_ROOT_DIR}/bkt2.d/key0.txt"
	echo -n dat21 > "${BUCKETS_ROOT_DIR}/bkt2.d/key1.txt"
}

geninput

./fs2kvpairs2rbat
