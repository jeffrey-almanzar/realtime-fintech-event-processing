#!/usr/bin/env bash
# Checks the running system from the outside, through its public APIs only:
# a deposit reaches the balance, a withdrawal settles, and a declined one is
# refunded by the workflow. Used by `make smoke` and by CI after `make demo`.
set -euo pipefail

EVENT_API=${EVENT_API:-http://localhost:8081}
QUERY_API=${QUERY_API:-http://localhost:8082}
ACCOUNT="smoke-$(date +%s)-$RANDOM"
TIMEOUT=${TIMEOUT:-90}

field() { # field <json> <key>: reads one top-level number from a flat JSON object
  sed -E "s/.*\"$2\":(-?[0-9]+).*/\1/" <<<"$1"
}

send() { # send <type> <amount>: posts a transaction and fails on anything but 202
  local code
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$EVENT_API/transactions" \
    -H 'Content-Type: application/json' \
    -d "{\"account_id\":\"$ACCOUNT\",\"type\":\"$1\",\"amount_minor\":$2,\"currency\":\"USD\"}")
  [ "$code" = 202 ] || { echo "FAIL: $1 $2 returned HTTP $code"; exit 1; }
}

wait_for_balance() { # wait_for_balance <expected> <description>
  local body balance="" deadline=$((SECONDS + TIMEOUT))
  while [ "$SECONDS" -lt "$deadline" ]; do
    body=$(curl -s "$QUERY_API/accounts/$ACCOUNT" || true)
    balance=$(field "$body" balance_minor)
    if [ "$balance" = "$1" ]; then
      echo "PASS: $2 (balance $1)"
      return 0
    fi
    sleep 1
  done
  echo "FAIL: $2: balance is '$balance', want $1"
  exit 1
}

wait_for_settlement() { # wait_for_settlement <status> <description>
  local deadline=$((SECONDS + TIMEOUT))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if curl -s "$QUERY_API/accounts/$ACCOUNT/transactions?limit=20" | grep -q "\"settlement\":\"$1\""; then
      echo "PASS: $2 (settlement $1)"
      return 0
    fi
    sleep 1
  done
  echo "FAIL: $2: no settlement reached '$1'"
  curl -s "$QUERY_API/accounts/$ACCOUNT/transactions?limit=20" || true
  exit 1
}

echo "Testing account $ACCOUNT"
send deposit 100000
wait_for_balance 100000 "deposit reached the balance"

send withdrawal 2500
wait_for_balance 97500 "withdrawal debited the account"
wait_for_settlement settled "withdrawal settled through Temporal"

send withdrawal 1013 # the mock provider declines amounts ending in 13
wait_for_settlement reversed "declined withdrawal was reversed"
wait_for_balance 97500 "refund restored the balance"

echo "All checks passed"
