#!/bin/bash
cat target/output/report | sort | awk -F\t '{print $2 " " $1 " " $4}' | awk '{print $1 " " $2 " " $3}' | grep $1 | sort -n -r | head -n $2 | awk '{print $3}'
