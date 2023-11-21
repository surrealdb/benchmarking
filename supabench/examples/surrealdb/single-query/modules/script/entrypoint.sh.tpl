#!/bin/bash

set -ueox pipefail
export PATH=$PATH:/usr/local/go/bin:~/.bin

# OPTIONAL: rebuild k6 with xk6 plugins, you may add some extra plugins here if needed
# The AMI comes with a compiled k6 binary with the xk6-output-prometheus-remote plugin
export K6_VERSION='v0.37.0'
~/go/bin/xk6 build --output /home/ubuntu/.bin/k6 \
  --with github.com/grafana/xk6-output-prometheus-remote@latest

# go to k6 dir and run k6
cd /tmp/k6 || exit 1

# leave these as is. Supabench will pass it and it is needed to upload the report.
export RUN_ID="${testrun_id}"
export BENCHMARK_ID="${benchmark_id}"
export TEST_RUN="${testrun_name}"
export TEST_ORIGIN="${test_origin}"
export SUPABENCH_TOKEN="${supabench_token}"
export SUPABENCH_URI="${supabench_uri}"

# this is the place to add your variables, required by benchmark.
export SUT_URL="${sut_url}"
export SUT_USERNAME="${sut_username}"
export SUT_PASSWORD="${sut_password}"
export QUERY="${sut_query}"
export NS="${sut_ns}"
export DB="${sut_db}"

# make command from the k6 folder to run k6 benchmark, you can add some extra vars here if needed
# Leave testrun_name as it is passed to k6 command to add global tag to all metrics for grafana!
make run \
  duration="${duration}" \
  vus="${vus}" \
  testrun="${testrun_name}" \
  testid="${testrun_id}" \
  instance_id="${instance_id}"
