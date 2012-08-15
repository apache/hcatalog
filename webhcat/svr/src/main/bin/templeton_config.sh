#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#====================================
#Default config param values
#====================================

# The file containing the running pid
PID_FILE=./templeton.pid

#default log directory
TEMPLETON_LOG_DIR=${TEMPLETON_LOG_DIR:-.}

# The console error log
ERROR_LOG=${TEMPLETON_LOG_DIR}/templeton-console-error.log

# The console log
CONSOLE_LOG=${TEMPLETON_LOG_DIR}/templeton-console.log

# The name of the templeton jar file
TEMPLETON_JAR=webhcat-0.5.0-SNAPSHOT.jar

# How long to wait before testing that the process started correctly
SLEEP_TIME_AFTER_START=10

#================================================
#See if the default configs have been overwritten
#================================================

#These parameters can be overriden by templeton-env.sh
# the root of the TEMPLETON installation
export TEMPLETON_PREFIX=`dirname "$this"`/..

#check to see if the conf dir is given as an optional argument
if [ $# -gt 1 ]
then
    if [ "--config" = "$1" ]
          then
              shift
              confdir=$1
              shift
              TEMPLETON_CONF_DIR=$confdir
    fi
fi

# Allow alternate conf dir location.
if [ -e "${TEMPLETON_PREFIX}/conf/templeton-env.sh" ]; then
  DEFAULT_CONF_DIR=${TEMPLETON_PREFIX}/"conf"
else
  DEFAULT_CONF_DIR="/etc/hcatalog"
fi
TEMPLETON_CONF_DIR="${TEMPLETON_CONF_DIR:-$DEFAULT_CONF_DIR}"

#users can add various env vars to templeton-env.sh in the conf
#rather than having to export them before running the command
if [ -f "${TEMPLETON_CONF_DIR}/templeton-env.sh" ]; then
  . "${TEMPLETON_CONF_DIR}/templeton-env.sh"
fi

#====================================
#determine where hadoop is
#====================================

#check HADOOP_HOME and then check HADOOP_PREFIX
if [ -f ${HADOOP_HOME}/bin/hadoop ]; then
  HADOOP_PREFIX=$HADOOP_HOME
#if this is an rpm install check for /usr/bin/hadoop
elif [ -f ${TEMPLETON_PREFIX}/bin/hadoop ]; then
  HADOOP_PREFIX=$TEMPLETON_PREFIX
#otherwise see if HADOOP_PREFIX is defined
elif [ ! -f ${HADOOP_PREFIX}/bin/hadoop ]; then
  echo "Hadoop not found."
  exit 1
fi





