#!/bin/bash

if [[ -z "$ZOOBINDIR" ]]
then
	echo "Error!! ZOOBINDIR is not set" 1>&2
	exit 1
fi

. $ZOOBINDIR/zkEnv.sh

# TODO Include your ZooKeeper connection string here. Make sure there are no spaces.
# 	Replace with your server names and client ports.
export ZKSERVER=open-gpu-1.cs.mcgill.ca:21810,open-gpu-6.cs.mcgill.ca:21810,open-gpu-23.cs.mcgill.ca:21810

java -cp $CLASSPATH:../task:.: DistClient "$@"
