// some k6 related imports
import http from 'k6/http'
import { check, fail } from 'k6';
// import ws from 'k6/ws';
import { WebSocket } from 'k6/experimental/websockets';

// you can use some common things for k6
// 'scenario' provides you the load scenario with ramping-vus executor and 2 periods of const load
// 'trends' is just a set of useful trends to be used in summary result like p95, med, p0.01
import { scenario, trends } from './common.js'

// export handleSummary from sumary.js to upload the report to Supabench
export { handleSummary } from './summary.js'

// you may access the environment variables specified in entrypoint.sh.tpl with __ENV.VAR_NAME
const username = __ENV.SUT_USERNAME ? __ENV.SUT_USERNAME : 'root'
const password = __ENV.SUT_PASSWORD ? __ENV.SUT_PASSWORD : 'root'
const base_url = __ENV.SUT_URL ? __ENV.SUT_URL : 'ws://localhost:8000/rpc'
const query = __ENV.QUERY ? __ENV.QUERY : 'LIVE SELECT * FROM table';
const ns = __ENV.NS ? __ENV.NS : 'k6';
const db = __ENV.DB ? __ENV.DB : 'k6';

// I recommend you to not remove this variable. So you will be able to tweak test duration.
const baseDuration = __ENV.DURATION ? __ENV.DURATION : 60;
const duration = parseInt(baseDuration) + 15;
const sessionDuration = __ENV.SESS_DURATION ? __ENV.SESS_DURATION : 10;
const sessDuration = parseInt(sessionDuration);

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
    return {
        base_url: base_url,
        query: query,
        tags: {
            base_url: base_url,
            query: query,
        }
    };
}

function signin(ws) {
    ws.addEventListener("open", () => {
        console.log("WS opened connection event")
        const request_id = "signin_request_id";

        // Set up listener before things can break
        ws.addEventListener('close', (e) => {
            console.log(`Websocket closed: ${e}`)
        })

        // And response listener
        ws.addEventListener('message', (e) => {
            const msg = JSON.parse(e.data)
            if (msg.id === request_id) {
                if (!check(msg, {
                    'signin must not be an error': m => m.error === null
                })) {
                    fail(`signin response had an error: ${msg}`)
                } else {
                    console.log(`Successful signin: ${msg}`)
                }
            }
            console.log(`received message: ${msg}`)
        })

        // Now send message we expect response for
        ws.send(JSON.stringify({
            id: request_id,
            method: "signin",
            params: [ {
                user: username,
                pass: password,
                ns: ns,
                db: db,
                sc: null,
            }],
        }))

    })
}

function create_lq(ws, query) {
    const lq_req_id = "live_query_reqid_"+Math.random() * 10000
    ws.send(JSON.stringify({
        id: lq_req_id,
        method: "query",
        params: [ query ]
    }))

    var return_live_query_id = null;
    var error = null;
    ws.addEventListener("message", (e) => {
        const msg = JSON.parse(e.data)
        if (msg.id === lq_req_id) {
            if (msg.error !== null) {
                error = msg.error
            } else if (msg.result !== null) {
                return_live_query_id = msg.result
            } else {
                error = `unhandled response for a live query creation: req_id=${lq_req_id} msg=${msg}`
            }
        }
    })

    const polling_rate_ms = 100;
    const internal = setInterval(() => {
        if (return_live_query_id !== null || error !==null) {
            clearInterval(internal)
        }
    }, polling_rate_ms)

    return {
        lq_req_id: lq_req_id,
        error: error,
    }
}

function write_query(ws, period_query) {
    const write_query_id = "write_query_reqid_"+Math.random()*10000
    ws.send(JSON.stringify({
        id: write_query_id,
        method: "query",
        params: [period_query]
    }))
    ws.addEventListener("message", (e) => {
        const msg = JSON.parse(e.data)
        if (msg.id === write_query_id) {
            check(msg, {
                "is not error": (m) => m.error === null,
                "is result": (m) => m.result !== null,
            })
        }
        // TODO handle lq response as well, by checking live query id
    })
}

export default function(setup) {
    console.log(`Creating websocket to ${base_url}`)
    const ws = new WebSocket(`${base_url}`);
    ws.addEventListener('open', () => {
        console.log("This is the immediate open event")
    })

    console.log(`Signing in`)
    signin(ws)

    // Create Live Query
    console.log(`Creating live query`)
    const lq = create_lq(ws, setup.query)

    // Create record
    const period_write_ms = 1000;
    const period_query = "CREATE table CONTENT {'name': 'some name'}"
    console.log(`Creating poller for writes`)
    const write_interval = setInterval(() => {
        console.log(`Writing query`)
        write_query(ws, period_query)
    }, period_write_ms)
    const write_timeout_ms = 10000;
    setTimeout(() => {
        console.log(`Removing write poller`)
        clearInterval(write_interval)
    }, write_timeout_ms)

    // Kill Live Query
    // TODO
    console.log(`End`)
}
