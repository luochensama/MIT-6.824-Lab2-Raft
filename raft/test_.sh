#!/bin/bash
i=0
while [ $i -lt 10 ]
do
	go test -race -run 2B
	sleep 1
	((i = i+1))
done
