// Load test for the Event API: ramps by arrival rate, because an event pipeline
// is sized in events per second rather than concurrent users.
//
//   k6 run loadtest/k6.js
//   k6 run -e TARGET=2000 -e HOLD=2m loadtest/k6.js
//
// This measures the HTTP edge only. End-to-end latency and consumer lag are in
// Grafana. To test the Processor beyond what HTTP can drive, use
// `make loadgen ARGS="-direct -rate 5000"`.
import http from 'k6/http';
import { check } from 'k6';
import { randomIntBetween, uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const API = __ENV.API_URL || 'http://localhost:8081';
const TARGET = Number(__ENV.TARGET || 5000);
const HOLD = __ENV.HOLD || '10m';
const ACCOUNTS = Number(__ENV.ACCOUNTS || 1000);

export const options = {
  scenarios: {
    events: {
      executor: 'ramping-arrival-rate',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 500,
      maxVUs: 2000,
      stages: [
        { duration: '2m', target: TARGET }, // ramp up
        { duration: HOLD, target: TARGET }, // sustain
      ],
    },
  },
  // Match the targets in the README.
  thresholds: {
    http_req_duration: ['p(99)<30'],
    http_req_failed: ['rate<0.001'],
    dropped_iterations: ['count==0'],
  },
};

export default function () {
  const res = http.post(
    `${API}/transactions`,
    JSON.stringify({
      event_id: uuidv4(),
      account_id: `acct-${randomIntBetween(0, ACCOUNTS - 1)}`,
      type: 'deposit',
      amount_minor: randomIntBetween(1, 100000),
      currency: 'USD',
    }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  check(res, { 'accepted (202)': (r) => r.status === 202 });
}
