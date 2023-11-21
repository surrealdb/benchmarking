// some k6 related imports
import http from 'k6/http'
import { check, fail } from 'k6';
// import ws from 'k6/ws';
import { WebSocket } from 'k6/experimental/websockets';
import { setInterval, setTimeout, clearInterval, clearTimeout } from 'k6/experimental/timers';

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
// NOT IN MILLISECONDS BECAUSE scenario ACCEPTS SECONDS
const duration = parseInt(baseDuration) + 15 ;
const sessionDuration = __ENV.SESS_DURATION ? __ENV.SESS_DURATION : 10;
// MILLISECONDS
const sessDuration = parseInt(sessionDuration) * 1000;

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


function generate_state() {
    return {
        // stage of the state machine of this test
        stage: STATE_STAGE_SIGNING_IN,
        // populated during the live query stage
        live_id : null,
        // numbers that repr intervals that need to be cleared
        intervals_to_kill: {},
        // numbers that repr timeouts that need to be cleared
        timeouts_to_kill: {},
        // map of request IDs that need answering
        pending_responses: {},
        // we want to keep track of this, so we know when state is completed
        // it contains request ids that haven't been sent yet.
        // Unused, but implemented
        pending_requests: {},
        expected_notifications: {},
    };
}

export function setup() {
    debug(`Setting up the test with base_url=${base_url} and query=${query}`);
    return {
        base_url: base_url,
        query: query,
        tags: {
            base_url: base_url,
            query: query,
        }
    };
}

const STATE_STAGE_SIGNING_IN = 0;
const STATE_STAGE_USE_SCOPE = 1;
const STATE_STAGE_CREATING_LIVE_QUERY = 2;
const STATE_STAGE_CREATING_DATA = 3;
const STATE_STAGE_CLEANUP = 4;

// Used to transition between handlers without a message required from WS
const EMPTY_MESSAGE = {data: "{}"}

const log_level = "TRACE";
const levels = [
    "INFO",
    "DEBUG",
    "TRACE",
]

function debug(msg) {
    label_log(msg, "DEBUG")
}

function info(msg) {
    label_log(msg, "INFO")
}

function trace(msg) {
    label_log(msg, "TRACE")
}

function label_log(msg, level) {
    for (const key of levels) {
        if (key === level) {
            console.log(msg)
        }
        if (key === log_level) {
            break; // it means the level is more-detailed than current
        }
    }
}

function createOnMessageStateHandler(ws, state) {
    // Could be an array, but isn't for the sake of clarity
    const state_handler_mapping= {
        [STATE_STAGE_SIGNING_IN]: on_msg_signin_in,
        [STATE_STAGE_USE_SCOPE]: on_msg_use_scope,
        [STATE_STAGE_CREATING_LIVE_QUERY]: on_msg_live_query,
        [STATE_STAGE_CREATING_DATA]: on_msg_creating_data,
        [STATE_STAGE_CLEANUP]: on_msg_cleanup,
    }
    return (e) => {
        var ret_msg = e;
        while (ret_msg !== undefined && ret_msg !== null) {
            trace(`Handling inbound message: ${JSON.stringify(ret_msg)}, state=${JSON.stringify(state)}`)
            // If the handler returns a value, that is the message for the next invocation
            ret_msg = state_handler_mapping[state.stage](ws, state, ret_msg)
            trace(`Now the ret is: ${JSON.stringify(ret_msg)}, and state is ${JSON.stringify(state)}`)
        }
    }
}

function on_msg_signin_in(ws, state, e) {
    const msg = JSON.parse(e.data)
    const request_id = "signin_request_id";
    if (Object.keys(msg).length === 0) {
        debug("Empty message, signing in")
        ws.send(JSON.stringify({
            id: request_id,
            method: "signin",
            params: [ {
                user: username,
                pass: password,
                ns: ns,
                db: db,
                // sc: null,
            }],
        }))
    } else {
        if (check(msg, {
            "is response to signin": (m) => m.id === request_id,
            "is valid": (m) => m.result !== null,
        })) {
            debug("Successfully signed in, changing state")
            state.stage = STATE_STAGE_USE_SCOPE
            return EMPTY_MESSAGE
        } else {
            debug("Unsuccessful signin, failing")
            fail(`Failed to sign in because received message ${e.data}`)
        }
    }
}

function on_msg_use_scope(ws, state, e) {
    const use_req_id = "use_req_id";
    if (e === EMPTY_MESSAGE) {
        debug("Transitioned into signed in state, sending USE request")
        ws.send(JSON.stringify({
            id: use_req_id,
            method: "use",
            params: [
                ns, db
            ],
        }))
    } else {
        const msg = JSON.parse(e.data)
        if (check(msg, {
            "is response to use request": (m) => m.id === use_req_id,
            "is successful": (m) => m.result === null,
        })) {
            state.stage = STATE_STAGE_CREATING_LIVE_QUERY
            debug("Transitioned into used state")
            return EMPTY_MESSAGE
        } else {
            fail(`Failed to invoke USE, msg: ${e.data}`)
        }
    }
}

function on_msg_live_query(ws, state, e) {
    const lq_req_id = "live_query_request"
    if (e === EMPTY_MESSAGE) {
        debug("Sending live query request")
        ws.send(JSON.stringify({
            id: lq_req_id,
            method: "query",
            params: [ query ]
        }))
    } else {
        const msg = JSON.parse(e.data)
        if (check(msg, {
            "is live query response": (m) => m.id === lq_req_id,
            "has result": (m) => m.result !==null,
            "has live query uuid": (m) => m.result[0].result !== null,
        })) {
            state.live_id = msg.result[0].result
            state.stage = STATE_STAGE_CREATING_DATA
            return EMPTY_MESSAGE
        }
    }
}

function on_msg_creating_data(ws, state, e) {
    const burst = true;
    const burst_number = 50;
    const write_timeout_ms = 10000;
    const period_query = "CREATE table CONTENT {'name': 'some name'}"
    if (e === undefined || e === EMPTY_MESSAGE) {
        if (burst) {
            debug(`Creating ${burst_number} requests`)
            for (let i=0; i<burst_number; i++) {
                const req_id = "write_query_reqid_"+Math.floor(Math.random()*10000)
                state.pending_responses[req_id] = true;
                ws.send(JSON.stringify({
                    id: req_id,
                    method: "query",
                    params: [period_query]
                }))
            }
            debug(`Created ${burst_number} requests`)
            // Set a timeout to stop waiting for responses
            state.timeouts_to_kill[setTimeout(() => {
                state.stage = STATE_STAGE_CLEANUP
            }, write_timeout_ms)] = true;
        } else {
            // This is the signal to create data
            const period_write_ms = 1000;

            debug(`Creating poller for writes`)
            const write_interval = setInterval(() => {
                debug(`Writing query`)
                const req_id = "write_query_reqid_"+Math.floor(Math.random()*10000)
                state.pending_responses[req_id] = true;
                ws.send(JSON.stringify({
                    id: req_id,
                    method: "query",
                    params: [period_query]
                }))
            }, period_write_ms)

            setTimeout(() => {
                debug(`Removing write poller and closing connection`)
                delete state.intervals_to_kill[write_interval]
                clearInterval(write_interval)
            }, write_timeout_ms)

            // Wait for remaining responses to come in
            const wind_down_wait_ms = 1000;
            setTimeout(() => {
                debug("Finished waiting for remaining responses")
                state.stage = STATE_STAGE_CLEANUP
                // Since we can't 'return' from here, we send an unnecessary request to guarantee a response
                // and trigger the next handler
                ws.send(JSON.stringify({
                    id: "invoke-next-stage",
                    method: "query",
                    params: ["INFO"], // TODO correct syntax or change to something easy
                }))
            }, write_timeout_ms+wind_down_wait_ms)

            // We don't need to wait for the timeout, we can change state immediately if we know we aren't waiting for anything
            if (burst && Object.keys(state.pending_responses).length===0) {
                state.stage = STATE_STAGE_CLEANUP
            }
        }
    } else {
        // This is where we will get live query notifications and create responses
        debug(`Received message during creation: ${e.data}`)
        let msg = JSON.parse(e.data);
        if (is_msg_notification(msg)) {
            // Check if for us
            check(msg, {
                "live query id matches": (m) => m.result.id === state.live_id,
            })
            for (const key of state.expected_notifications) {
                if (e.data.includes(key)) {
                    delete state.expected_notifications[key]
                    break;
                }
            }
        } else {
            if (check(msg, {
                "received creation message is not error": (m) => !("error" in m),
                "received creation message is result": (m) => "result" in m,
            })) {
                delete state.pending_responses[msg.id]
            }
        }
        if (burst && Object.keys(state.pending_responses).length===0 && Object.keys(state.expected_notifications)) {
            state.stage = STATE_STAGE_CLEANUP
        }
    }
}

function is_msg_notification(msg) {
    if ("result" in msg) {
        const result = msg.result
        if ("action" in msg) {
            return true;
        }
    }
}

function is_msg_response(msg) {
    if ("result" in msg) {
        const result = msg.result;
        if ("result" in result) {
            return true
        }
    }
}

function on_msg_cleanup(ws, state, e) {
    // TODO Kill Live Query
    for (const interval in state.intervals_to_kill) {
        clearInterval(interval)
        delete state.intervals_to_kill[interval]
    }
    for (const timeout in state.timeouts_to_kill) {
        clearTimeout(timeout)
        delete state.timeouts_to_kill[timeout]
    }
    // Validate end state
    check(state, {
        "no intervals": (st) => Object.keys(st.intervals_to_kill).length===0,
        "no timeouts": (st) => Object.keys(st.timeouts_to_kill).length===0,
        "no pending write responses": (st) => Object.keys(st.pending_responses).length===0,
        "no pending write requests": (st) => Object.keys(st.pending_requests).length===0,
        "no pending notification responses": (st) => Object.keys(st.expected_notifications).length === 0,
    })
    debug(`Completed scenario, closing connection. Pending responses = ${JSON.stringify(state.pending_responses)}`)
    ws.close()
}

// The main VU code
export default function(setup) {
    debug("start")
    const ws = new WebSocket(`${base_url}`);
    // State describes the stage of the protocol
    // This is declared outside of setup, because setup is run once for all VUs
    const state = generate_state()
    ws.onopen = () => {
        // Create and register state machine message handler
        const handler = createOnMessageStateHandler(ws, state)
        ws.onmessage = handler

        // Invoke it immediately, because we need to fake an on-connect
        handler({data: "{}"})
    }
    state.timeouts_to_kill[setTimeout(() => {
        // This is a failsafe to terminate the connection in case the test runs too long
        const msg = `Test ran too long and is being terminated early, state=${JSON.stringify(state)}`
        state.stage=STATE_STAGE_CLEANUP
        on_msg_cleanup(ws, state, EMPTY_MESSAGE)
        fail(msg)
    }, sessDuration)] = true
    // The function will immediately end, but the "session" lasts for as long as the connection is open
}
