#!/bin/bash
#
cd /tmp/foo
mkdir 1
cd  1
for a in {1..2000}
do
	touch $a
	printf "create %10d\r" $a
done
for a in {1..2000}
do
	rm $a
	printf "remove %10d\r" $a
done
for a in {1..2000}
do
	echo "foo" > $a
	printf "create %10d\r" $a
done
