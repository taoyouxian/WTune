#!/usr/bin/env bash
#
# run.sh
# Copyright (C) 2019 tao <tao@coder>
#
# Distributed under terms of the MIT license.
#


jar_path=target/boot/rainbow-server-0.1.0-SNAPSHOT-executable.jar
target_path=/home/tyx/opt/rainbow/
scp -P 5102 -r $jar_path tyx@183.174.228.162:$target_path

