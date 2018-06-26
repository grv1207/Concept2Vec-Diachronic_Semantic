#!/bin/bash
line_num=1

while read line;do
  printf "$line_num: $(echo $line | wc -w)"
  ((line_num++))
done
