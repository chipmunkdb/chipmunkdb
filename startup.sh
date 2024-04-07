#!/bin/sh


python3 -u index.py 2>&1 &
P1=$!

# node tumblerServer starten

wait $P1