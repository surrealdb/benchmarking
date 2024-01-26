// some k6 related imports
import http from 'k6/http'
import { check, fail, abort, sleep} from 'k6';
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
const sessionDuration = __ENV.SESS_DURATION ? __ENV.SESS_DURATION : duration;
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

// Random test id
var run_id = `runid-unset-runid`;

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
        debug_requests_responses: [],
        // Sometimes we will receive notifications before we transition state properly
        received_notifications: [],
        // We declare the test timeout, so that we can remove it if the test completes successfully
        // But not remove it if the test fails due to this timeout
        test_timeout: undefined,
    };
}

function debug_req(msg) {
    return {
        type: "request",
        when: new Date(Date.now()).toISOString(),
        content: msg,
    }
}

function debug_resp(msg) {
    return {
        type: "response",
        when: new Date(Date.now()).toISOString(),
        content: msg,
    }
}

function debug_fake(msg) {
    return {
        type: "fake",
        when: new Date(Date.now()).toISOString(),
        content: msg,
    }
}

export function setup() {
    debug(`Setting up the test with base_url=${base_url} and query=${query}`);
    return {
        base_url: base_url,
        query: query,
        ns: ns,
        db: db,
        tags: {
            base_url: base_url,
            query: query,
        }
    };
}

// Initial state, during which we sign in
const STATE_STAGE_SIGNING_IN = 0;
// Having signed in, we send a USE request
const STATE_STAGE_USE_SCOPE = 1;
// Then we establish a live query
const STATE_STAGE_CREATING_LIVE_QUERY = 2;
// Create and capture data for the live query
const STATE_STAGE_CREATING_DATA = 3;
// Remove all timers, perform checks
const STATE_STAGE_CLEANUP = 4;
// Similar to cleanup stage, but avoids removing test timeouts
const STATE_STAGE_TIMEOUT = 5;

// Used to transition between handlers without a message required from WS
// String value used because we JSON.parse data
const EMPTY_MESSAGE = {data: "{}"}

const log_level = "DEBUG";
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
    const m = label_log_str(msg, level)
    if (m !== undefined) {
        console.log(m)
    }
}

function label_log_str(msg, level) {
    for (const key of levels) {
        if (key === level) {
            return `[${run_id}] ${msg}`
        }
        if (key === log_level) {
            break; // it means the level is more-detailed than current
        }
    }
}

function createOnMessageStateHandler(ws, setup, state) {
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
        state.debug_requests_responses.push(debug_resp(e.data))
        while (ret_msg !== undefined && ret_msg !== null) {
            if (ret_msg!==e) {
                state.debug_requests_responses.push(debug_fake(ret_msg.data))
            }
            trace(`Handling inbound message: ${JSON.stringify(ret_msg)}, state=${JSON.stringify(state)}`)
            // If the handler returns a value, that is the message for the next invocation
            ret_msg = state_handler_mapping[state.stage](ws, setup, state, ret_msg)
            trace(`Now the ret is: ${JSON.stringify(ret_msg)}, and state is ${JSON.stringify(state)}`)
        }
    }
}

function on_msg_signin_in(ws, setup, state, e) {
    const msg = JSON.parse(e.data)
    const request_id = `signin_request_id_${run_id}`;
    if (Object.keys(msg).length === 0) {
        debug("Empty message, signing in")
        const send_msg = JSON.stringify({
            id: request_id,
            method: "signin",
            params: [ {
                user: username,
                pass: password,
                // ns: setup.ns,
                // db: setup.db,
                // sc: null,
            }],
        })
        state.debug_requests_responses.push(debug_req(send_msg))
        ws.send(send_msg)
    } else {
        if (check(msg, {
            "is response to signin": (m) => m.id === request_id,
            "is signin result": (m) => "result" in m,
            "is not signin error": (m) => !("error" in m)
        })) {
            debug("Successfully signed in, changing state")
            state.stage = STATE_STAGE_USE_SCOPE
            return EMPTY_MESSAGE
        } else {
            debug(`Unsuccessful signin, failing: ${e.data}`)
            end_test(ws, state, `Failed to sign in because received message ${e.data}`)
        }
    }
}

function on_msg_use_scope(ws, setup, state, e) {
    const use_req_id = `use_req_id_${run_id}`;
    if (e === EMPTY_MESSAGE) {
        debug("Transitioned into signed in state, sending USE request")
        const send_msg = JSON.stringify({
            id: use_req_id,
            method: "use",
            params: [
                ns, db
            ],
        })
        state.debug_requests_responses.push(debug_req(send_msg))
        ws.send(send_msg)
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
            end_test(ws, state, `Failed to invoke USE, msg: ${e.data}`)
        }
    }
}

function on_msg_live_query(ws, setup, state, e) {
    const lq_req_id = `live_query_request_${run_id}`
    if (e === EMPTY_MESSAGE) {
        debug("Sending live query request")
        const send_msg = JSON.stringify({
            id: lq_req_id,
            method: "query",
            params: [ query ]
        })
        state.debug_requests_responses.push(debug_req(send_msg))
        ws.send(send_msg)
    } else {
        const msg = JSON.parse(e.data)
        debug(`The message is ${e.data}`)

        if (is_msg_notification(msg)) {
            // Sometimes we can receive notification responses before we receive the live query response?
            state.received_notifications.push(msg)
        } else if (check(msg, {
            "is live query response": (m) => m.id === lq_req_id,
            "live query response has result (i.e. no error)": (m) => (("result" in m)),
            "has live query uuid": (m) => (
                ("result" in m) && // m.result exists
                Array.isArray(m.result) && // m.result is array
                (Object.keys(m.result).length>0) && // m.result has an item
                ("result" in m.result[0]) && // result has a result
                (m.result[0].result !== null)), // the result's result is not empty
        })) {
            state.live_id = msg.result[0].result
            state.stage = STATE_STAGE_CREATING_DATA
            return EMPTY_MESSAGE
        } else {
            // Since we are assuming that the test is linear, we aren't going to receive another message

            end_test(ws, state, `Did not receive expected live query response: ${e.data}`)
        }
    }
}

function on_msg_creating_data(ws, setup, state, e) {
    const burst = true;
    const burst_number = 50;
    const write_timeout_ms = 10000;
    const period_query = "CREATE table CONTENT {'name': 'some name'}"
    if (e === undefined || e === EMPTY_MESSAGE) {
        if (burst) {
            debug(`Creating ${burst_number} requests`)
            for (let i=0; i<burst_number; i++) {
                const rnd = Math.floor(Math.random()*10000)
                const req_id = `write_query_reqid_${rnd}_${run_id}`

                state.pending_responses[req_id] = true;
            }
            for (const req_id of Object.keys(state.pending_responses)) {
                const send_msg = JSON.stringify({
                    id: req_id,
                    method: "query",
                    params: [period_query]
                })
                state.debug_requests_responses.push(debug_req(send_msg))
                ws.send(send_msg)
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
                const rnd = Math.floor(Math.random()*10000)
                const req_id = `write_query_reqid_${rnd}_${run_id}`
                state.pending_responses[req_id] = true;
                const send_msg =JSON.stringify({
                    id: req_id,
                    method: "query",
                    params: [period_query]
                })
                state.debug_requests_responses.push(debug_req(send_msg))
                ws.send(send_msg)
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
                const send_msg = JSON.stringify({
                    id: "invoke-next-stage",
                    method: "query",
                    params: ["INFO FOR DB"], // TODO correct syntax or change to something easy
                })
                state.debug_requests_responses.push(debug_req(send_msg))
                ws.send(send_msg)
            }, write_timeout_ms+wind_down_wait_ms)

            // We don't need to wait for the timeout, we can change state immediately if we know we aren't waiting for anything
            if (burst && Object.keys(state.pending_responses).length===0) {
                state.stage = STATE_STAGE_CLEANUP
            }
        }
    } else {
        // This is where we will get live query notifications and create responses
        trace(`Received message during creation: ${e.data}`)
        let msg = JSON.parse(e.data);
        if (is_msg_notification(msg)) {
            // Check if for us
            check(msg, {
                "live query id matches": (m) => m.result.id === state.live_id,
            })
            for (const key of Object.keys(state.expected_notifications)) {
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
            const send_msg = JSON.stringify({
                id: "invoke-next-stage-after-collecting-results",
                method: "query",
                params: ["INFO FOR DB"], // TODO correct syntax or change to something easy
            })
            state.debug_requests_responses.push(debug_req(send_msg))
            ws.send(send_msg)
        }
    }
}

function is_msg_notification(msg) {
    return ("result" in msg) &&
        ("action" in msg.result)
}

function is_msg_response(msg) {
    return ("result" in msg) &&
        ("result" in msg.result)
}

function on_msg_cleanup(ws, setup, state, e) {
    // TODO Kill Live Query
    end_test(ws, state, undefined)
}

function end_test(ws, state, failmsg) {
    // Validate end state
    check(state, {
        "no intervals": (st) => Object.keys(st.intervals_to_kill).length===0,
        "no timeouts": (st) => Object.keys(st.timeouts_to_kill).length===1,
        "no pending write responses": (st) => Object.keys(st.pending_responses).length===0,
        "no pending write requests": (st) => Object.keys(st.pending_requests).length===0,
        "no pending notification responses": (st) => Object.keys(st.expected_notifications).length === 0,
    })
    debug(`Completed scenario, closing connection. Pending responses = ${JSON.stringify(state.pending_responses)}`)
    for (const interval in state.intervals_to_kill) {
        clearInterval(interval)
        delete state.intervals_to_kill[interval]
    }
    for (const timeout in state.timeouts_to_kill) {
        clearTimeout(timeout)
        delete state.timeouts_to_kill[timeout]
    }
    if (state.stage !== STATE_STAGE_TIMEOUT) {
        debug("State wasnt timeout so cleared timeout")
        clearTimeout(state.test_timeout)
    } else {
        debug("State was timeout, so didnt clear timeout")
    }
    if (failmsg !== undefined) {
        fail(failmsg)
    }
    ws.close()
}

// The main VU code
export default function(setup) {
    // Set the runid here, because otherwise the init step of k6 shares the value(??)
    run_id = `runid-${Math.floor(Math.random()*10000)}-runid`
    debug("start")
    const ws = new WebSocket(`${base_url}`);
    // State describes the stage of the protocol
    // This is declared outside of setup, because setup is run once for all VUs
    const state = generate_state()
    ws.onopen = () => {
        // Create and register state machine message handler
        debug("connected")
        const handler = createOnMessageStateHandler(ws, setup, state)
        ws.onmessage = handler

        // Invoke it immediately, because we need to fake an on-connect
        handler(EMPTY_MESSAGE)
    }
    const test_timeout = setTimeout(() => {
        // This is a failsafe to terminate the connection in case the test runs too long
        const msg = label_log_str(`Test ran too long and has been terminated early, state=${JSON.stringify(state)}`, "INFO")
        state.stage=STATE_STAGE_TIMEOUT
        for (var i=0; i<state.debug_requests_responses.length; i++) {
            debug(`[${i}]: ${JSON.stringify(state.debug_requests_responses[i])}`)
        }
        debug("script failing scenario due to timeout")
        end_test(ws, state, msg)
    }, sessDuration)
    state.test_timeout = test_timeout;
    // The function will immediately end, but the "session" lasts for as long as the connection is open
    // https://k6.io/docs/using-k6/protocols/websockets/
}
