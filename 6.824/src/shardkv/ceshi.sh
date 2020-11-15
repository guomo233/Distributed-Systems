#!/bin/bash
for i in `seq 1 100`
do
    echo "test iter: $i"
    go test -run $1
    #if [ $(go test -run $1 | grep 'FAIL' | wc -l) -gt 0 ];
    #then
    #    break
    #fi
done
