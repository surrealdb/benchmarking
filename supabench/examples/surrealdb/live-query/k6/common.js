/**
 * Return a random integer between the minimum (inclusive)
 * and maximum (exclusive) values
 * @param {number} min - The minimum value to return.
 * @param {number} max - The maximum value you want to return.
 * @return {number} The random number between the min and max.
 */
export function getRandomInt(min, max) {
  min = Math.ceil(min)
  max = Math.floor(max)
  // The maximum is exclusive and the minimum is inclusive
  return Math.floor(Math.random() * (max - min) + min)
}

/**
 * Generate default k6 ramping-vus scenario.
 * @param {number} baseDuration - Total duration of the scenario.
 * @param {number} conns - max number of vus during the scenario execution.
 *
 * It starts with 0 VUs, ramps up to half the number of connections in 1/12 of total duration then
 * it remains on this number for 1/4 of total duration time.
 * Then ramps down to a quarter of the number of connections in 1/12 of total duration.
 * Then ramps up to the full number of connections in 1/6 of total duration and
 * it remains on this number for 1/3 of total duration time.
 * Then ramps down to a quarter of the number of connections in 1/12 of total duration,
 * then ramps down to 0 VUs in 10s.
 */
export function scenario(baseDuration, conns) {
  return {
    executor: 'ramping-vus',
    // startVUs: 1,
    startVUs: Math.floor(parseInt(conns)),
    stages: [
      // {
      //   duration: `${parseInt(baseDuration) / 2}s`,
      //   target: Math.floor(parseInt(conns)),
      // },
      {
        // duration: `${(parseInt(baseDuration)) / 2}s`,
        duration: `${(parseInt(baseDuration))}s`,
        target: Math.floor(parseInt(conns)),
      },
      { duration: `10s`, target: 0 },
    ],
    gracefulRampDown: '5s',
  }
}

/* Exporting an array of default summaryTrendStats to be used in summary result. */
export const trends = ['min', 'p(0.1)', 'avg', 'med', 'p(95)', 'p(99)', 'max', 'count']
