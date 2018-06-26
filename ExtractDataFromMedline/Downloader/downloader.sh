#!/bin/bash
cd $3
curl  $1 > $2
gzip -d $2

