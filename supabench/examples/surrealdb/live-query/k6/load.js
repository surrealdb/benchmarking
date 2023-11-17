// some k6 related imports
import http from 'k6/http'
import { check, fail } from 'k6';

// you can use some common things for k6
// 'scenario' provides you the load scenario with ramping-vus executor and 2 periods of const load
// 'trends' is just a set of useful trends to be used in summary result like p95, med, p0.01
import { scenario, trends } from './common.js'

// export handleSummary from sumary.js to upload the report to Supabench
export { handleSummary } from './summary.js'

// you may access the environment variables specified in entrypoint.sh.tpl with __ENV.VAR_NAME
const username = __ENV.SUT_USERNAME ? __ENV.SUT_USERNAME : 'root'
const password = __ENV.SUT_PASSWORD ? __ENV.SUT_PASSWORD : 'root'
const base_url = __ENV.SUT_URL ? __ENV.SUT_URL : 'http://localhost:8000'
const query = __ENV.QUERY ? __ENV.QUERY : 'SELECT * FROM table:1';
const ns = __ENV.NS ? __ENV.NS : 'k6';
const db = __ENV.DB ? __ENV.DB : 'k6';

// I recommend you to not remove this variable. So you will be able to tweak test duration.
const baseDuration = __ENV.DURATION ? __ENV.DURATION : 60;
const duration = parseInt(baseDuration) + 15;

// you may access the environment variables specified in make as well
const conns = __ENV.VUS_COUNT ? __ENV.VUS_COUNT : 1;

// k6 provides a lot of default metrics (https://k6.io/docs/using-k6/metrics/)
// But you may specify custom metrics like so if needed:
//
// const latencyTrend = new Trend('latency_trend')
// const counterReceived = new Counter('received_updates')

// specifying thresholds for the benchmark
const to = {}

// create options with 'scenario', 'trends' and 'to'
export const options = {
  vus: 1,
  thresholds: to,
  summaryTrendStats: trends,
  scenarios: {
    singleQuery: scenario(duration, conns),
  },
}

export function setup() {
    console.log(`Setting up the test with base_url=${base_url} and query=${query}`);
    const opts = {
        headers: {
            Accept: 'application/json',
        },
    };
    const res = http.post(`${base_url}/signin`, `{"user": "${username}","pass":"${password}"}`, opts);
    const checkRes = check(res, {
        'status is 200': (r) => r && r.status === 200,
    });
    if (!checkRes) {
        fail(`Unexpected response. status=${res.status} body=${res.body}`);
    }

    return {
        token: res.json().token,
        base_url: base_url,
        query: query,
        tags: {
            base_url: base_url,
            query: query,
        }
    };
}

export default function(setup) {
    const opts = {
        headers: {
            Authorization: `Bearer ${setup.token}`,
            Accept: 'application/json',
            NS: ns,
            DB: db,
        },
        tags: setup.tags,
    };
    const res = http.post(`${setup.base_url}/sql`, setup.query, opts);

    const checkRes = check(res, {
        'status is 200': (r) => r && r.status === 200,
    });
    if (!checkRes) {
        fail(`Unexpected response. status=${res.status} body=${res.body}`);
    }

    const checkResBody = check(res, {
        'result is OK': (r) => r.json()[0].status === "OK",
        'result has no error': (r) => r.json()[0].error === undefined,
    });
    if (!checkResBody) {
        fail(`Unexpected body. status=${res.status} body=${res.body}`);
    }
}
