#! /bin/bash


echo $1
cd $2
gzip -d $1
#cat $1 | grep 'utterance\|mappings' > $1.txt
#sed -i -e 's/mappings(\[\]).//g' $1.txt
#sed '/^\s*$/d' $1.txt > sprc.$1
#rm $1.txt

