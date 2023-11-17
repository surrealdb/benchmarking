.PHONY: load

MAKEFLAGS += -j2

export

duration ?= 600
vus ?= 10
prom_user ?= 1166612
prom_pass ?= glc_eyJvIjoiOTM0MjQ1IiwibiI6InN0YWNrLTcyOTM0OC1obS1yZWFkLXN1cGFiZW5jaCIsImsiOiJlbDFqSjJwMFY4OEVSeDFDM083TWs0MHUiLCJtIjp7InIiOiJwcm9kLWV1LXdlc3QtMiJ9fQ==
prom_url ?= https://prometheus-prod-24-prod-eu-west-2.grafana.net/api/prom/push

rand = $(shell bash -c 'echo $$RANDOM')
testrun ?= "random-run-$(rand)"
testid ?= "$(rand)"
instance_id ?= "$(shell hostname)"

run: load

load:
	@DURATION=$(duration) TEST_RUN=$(testrun) VUS_COUNT=$(vus) \
		K6_PROMETHEUS_RW_SERVER_URL=$(prom_url) \
		K6_PROMETHEUS_RW_USERNAME=$(prom_user) K6_PROMETHEUS_RW_PASSWORD=$(prom_pass) \
		K6_PROMETHEUS_RW_TREND_STATS='count,sum,min,max,avg,med,p(95),p(99)' \
		K6_PROMETHEUS_RW_PUSH_INTERVAL=5s \
		k6 run load.js --tag testrun=$(testrun) --tag instance_id=$(instance_id) --tag testid=$(testid) -o experimental-prometheus-rw --verbose
