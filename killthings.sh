#!/bin/bash
jps -l | grep fyodor | cut -d" " -f1 | xargs kill -9

