import http from 'k6/http';
import { check } from 'k6';

export let options = {
  stages: [
    { duration: '1m', target: 1000 }, // ramp-up
    { duration: '10m', target: 1000 }, // sustain
  ],
  thresholds: {
    http_req_duration: ['p(95)<200', 'p(99)<300'],
    http_req_failed: ['rate<0.01'],
  },
};

export default function () {
  let res = http.get('http://localhost:9092/financial-events');
  check(res, {
    'is status 200': (r) => r.status === 200,
  });
}