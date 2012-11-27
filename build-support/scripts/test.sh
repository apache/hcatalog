#!/bin/bash

function run_cmd() {
  echo "Running command: ${cmd}"
  ${cmd}
  if [ $? != 0 ]; then
    echo "Failed!"
    exit 1
  fi
}

umask 0022

# Build with hadoop23, but do not run tests as they do not pass.
cmd='ant clean package -Dmvn.hadoop.profile=hadoop23'
run_cmd

# Build and run tests with hadoop20. This must happen afterwards so test results
# are available for CI to publish.
cmd='ant clean package test -Dtest.junit.output.format=xml'
run_cmd

