#!/bin/bash

#This script is only used for testing. Its just an easy way of creating a small chord network.
#Modify the cd command below if you are running this script from the current directory.

numTerms=2
initialPort=7200

clear
cd PyChordDHT
gnome-terminal -x python node.py -p $initialPort
echo "Starting new node on port $initialPort"
for (( i=0 ; i<numTerms; i++ ))
do
	((newPort=initialPort + 1))
	echo "Starting new node on port $newPort"
	gnome-terminal -x python node.py -p $newPort -e localhost:$initialPort
	((initialPort++))
done
