#!/usr/bin/env bash
#
# Submit N jobs concurrently and report what the system did with them.
#
# Answers "how does this handle N simultaneous job requests?" with numbers
# rather than an assertion, and it deliberately
# reports refusals as a *success* of the design: a system that refuses work it
# cannot start, and says when to come back, is behaving correctly. A system
# that accepts all 50 and silently drops some is the failure mode.
#
# Usage:
#   ./scripts/load-test.sh [count] [server]
#
# Environment:
#   AIRLOCK_TOKEN   bearer token, if the server requires one
#   AGENT_COMMAND    command to run as the agent (process driver)
#   AGENT_IMAGE      image to run as the agent (docker driver)

set -uo pipefail

COUNT="${1:-50}"
SERVER="${2:-${AIRLOCK_SERVER:-http://localhost:8080}}"
TOKEN="${AIRLOCK_TOKEN:-}"

AGENT_IMAGE="${AGENT_IMAGE:-}"
AGENT_COMMAND="${AGENT_COMMAND:-$(pwd)/bin/airlock-agent}"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Expanded below as ${auth[@]+"${auth[@]}"} rather than "${auth[@]}".
# macOS ships bash 3.2, where expanding an *empty* array under `set -u` is an
# "unbound variable" error; bash 4.4+ (and so Linux CI) treats it as empty.
# Without the guard this script dies on every macOS run with no token set.
auth=()
if [ -n "$TOKEN" ]; then
  auth=(-H "Authorization: Bearer $TOKEN")
fi

if [ -n "$AGENT_IMAGE" ]; then
  spec="\"image\":\"$AGENT_IMAGE\""
else
  spec="\"command\":[\"$AGENT_COMMAND\"]"
fi

echo "submitting $COUNT jobs to $SERVER"
echo

started=$(date +%s)

# Submit everything at once. Sequential submission would measure the client,
# not the server, and the question is specifically about simultaneous requests.
for i in $(seq 1 "$COUNT"); do
  (
    response=$(curl -s -o "$WORK/body.$i" -w '%{http_code}' -X POST "$SERVER/v1/jobs" \
      ${auth[@]+"${auth[@]}"} \
      -H 'Content-Type: application/json' \
      -d "{$spec,\"env\":{\"AIRLOCK_STEP_DELAY\":\"100ms\"},\"priority\":\"normal\"}")
    echo "$response" > "$WORK/status.$i"
  ) &
done
wait

submit_elapsed=$(( $(date +%s) - started ))

accepted=0
rate_limited=0
queue_full=0
other=0

for i in $(seq 1 "$COUNT"); do
  status=$(cat "$WORK/status.$i" 2>/dev/null || echo 000)
  case "$status" in
    202) accepted=$((accepted + 1));;
    429) rate_limited=$((rate_limited + 1));;
    503) queue_full=$((queue_full + 1));;
    *)   other=$((other + 1));;
  esac
done

echo "submission results after ${submit_elapsed}s:"
echo "  accepted (202):      $accepted"
echo "  rate limited (429):  $rate_limited"
echo "  queue full (503):    $queue_full"
echo "  other:               $other"
echo

if [ "$rate_limited" -gt 0 ] || [ "$queue_full" -gt 0 ]; then
  echo "  note: refusals are the design working. Each carries a Retry-After;"
  echo "        accepting work the system cannot start would be the bug."
  echo
fi

if [ "$accepted" -eq 0 ]; then
  echo "nothing was accepted; is the control plane running at $SERVER?"
  exit 1
fi

echo "waiting for accepted jobs to finish..."

deadline=$(( $(date +%s) + 300 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  stats=$(curl -s ${auth[@]+"${auth[@]}"} "$SERVER/v1/stats")
  ready=$(echo "$stats" | grep -o '"ready":[0-9]*' | head -1 | cut -d: -f2)
  claimed=$(echo "$stats" | grep -o '"claimed":[0-9]*' | head -1 | cut -d: -f2)
  running=$(echo "$stats" | grep -o '"running":[0-9]*' | head -1 | cut -d: -f2)

  printf '\r  ready=%-4s claimed=%-4s running=%-4s' "${ready:-?}" "${claimed:-?}" "${running:-?}"

  if [ "${ready:-1}" = "0" ] && [ "${claimed:-1}" = "0" ]; then
    break
  fi
  sleep 1
done

echo
total_elapsed=$(( $(date +%s) - started ))
echo

succeeded=$(curl -s ${auth[@]+"${auth[@]}"} "$SERVER/v1/jobs?state=succeeded&limit=500" | grep -o '"count":[0-9]*' | cut -d: -f2)
failed=$(curl -s ${auth[@]+"${auth[@]}"} "$SERVER/v1/jobs?state=failed&limit=500" | grep -o '"count":[0-9]*' | cut -d: -f2)

echo "outcome after ${total_elapsed}s:"
echo "  succeeded: ${succeeded:-0}"
echo "  failed:    ${failed:-0}"
echo

if [ "${failed:-0}" != "0" ]; then
  echo "failure breakdown:"
  curl -s ${auth[@]+"${auth[@]}"} "$SERVER/v1/jobs?state=failed&limit=500" \
    | grep -o '"kind":"[a-z_]*"' | sort | uniq -c | sed 's/^/  /'
  echo
fi

echo "final scheduler state:"
curl -s ${auth[@]+"${auth[@]}"} "$SERVER/v1/stats" | sed 's/^/  /'
echo

# The cost-control claim, checked rather than asserted: nothing should be left
# running once the queue has drained.
leftover=$(curl -s ${auth[@]+"${auth[@]}"} "$SERVER/v1/stats" | grep -o '"running":[0-9]*' | head -1 | cut -d: -f2)
if [ "${leftover:-0}" != "0" ]; then
  echo "WARNING: $leftover job(s) still running after the queue drained"
  exit 1
fi

echo "queue drained, nothing left running"
