#!/usr/bin/env bash
#
# scp.sh
# Copyright (C) 2019 tao <tao@coder>
#
# Distributed under terms of the MIT license.
#
# Description: transfer rainbow.properties and config files like schema.txt, workload.txt

if [[ ( "$#" < 1 ) ]] ; then
    echo "Error: path is not given"
    echo ""
    echo "Usage: run command"
    echo "    scp.sh <path> <target_path>"
    echo "to scp file or directory to a remote host."
    echo "<path> is needed (e.g., /path/rainbow)."
    echo "<target_path> default is /home/tyx/rainbow/."
    exit 1
fi

if [[ ( "$#" == 2 ) ]] ; then
    target_path=$2
else
    target_path=/home/tyx/rainbow/
fi

echo "scp begin"

scp -P 5102 -r $1 tyx@183.174.228.162:$target_path

echo "scp okay"
