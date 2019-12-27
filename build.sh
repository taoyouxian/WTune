#!/usr/bin/env bash
#
# build.sh
# Copyright (C) 2019 tao <tao@coder>
#
# Distributed under terms of the MIT license.
#


mvn clean package -Dmaven.test.skip
bash run.sh

