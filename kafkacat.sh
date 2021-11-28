#!/bin/bash
kcat -b kafka:9092 -C -o beginning -q -t webcmds 
# kcat -b kafka:9092 -C -o beginning -q -t search_events 


