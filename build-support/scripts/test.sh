#!/bin/bash

# Run HCatalog tests. This script intends to centralize the different commands
# and options necessary to build and test HCatalog. It should be run before
# committing patches, and from CI.
#
# Set ANT_HOME to specify the version of ant to build with
# (feature of "ant" shell script, not implemented here).

function run_cmd() {
  echo "Running command: ${cmd}"
  ${cmd}
  if [ $? != 0 ]; then
    echo "Failed!"
    exit 1
  fi
}

if [ "${FORREST_HOME}" == "" ]; then
  echo "required environment variable FORREST_HOME not set"
  exit 1
fi

umask 0022
env

cmd='ant clean src-release'
run_cmd

cd build
tar -xzvf hcatalog-src-*.tar.gz
cd hcatalog-src-*
echo "Running tests from $(pwd)"

# Build with hadoop23, but do not run tests as they do not pass.
cmd='ant -v clean package -Dmvn.hadoop.profile=hadoop23'
run_cmd

# Build and run tests with hadoop20. This must happen afterwards so test results
# are available for CI to publish.
cmd='ant -v -Dtest.junit.output.format=xml clean package test'
if [ "${HUDSON_URL}" == "https://builds.apache.org/" ]; then
  cmd="${cmd} mvn-deploy"
fi
run_cmd
