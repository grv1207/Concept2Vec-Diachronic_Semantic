#! /bin/bash


echo $1
cd $2
cat $1 | grep 'utterance\|mappings' > 1
sed -i -e 's/mappings(\[\]).//g' 1
sed '/^\s*$/d' 1 > sprc.$1
rm 1
mv sprc.$1 $3
rm $1
