#!/bin/bash

for pid in $(ps x | grep "bin\/master\|bin\/server\|bin\/.*client.*" | awk '{ print $1 }'); do
    kill $pid
done
