#!/usr/bin/env bash
# Sign and send one webhook to the Receiver by hand — see exactly what goes over the wire.
# Usage: scripts/send-webhook.sh <type> <orderId> [eventId]
#   Reuse an eventId to send a duplicate.
set -euo pipefail

TYPE=${1:?type, e.g. order.created | payment.succeeded | payment.refunded}
ORDER_ID=${2:?orderId}
EVENT_ID=${3:-evt_$(uuidgen | tr -d '-' | tr '[:upper:]' '[:lower:]')}
SECRET=${WEBHOOK_SECRET:-dev-secret-change-me}
URL=${RECEIVER_URL:-http://localhost:5002/webhooks}

TIMESTAMP=$(date +%s)
BODY=$(printf '{"id":"%s","type":"%s","createdAt":"%s","data":{"orderId":%s,"amount":50.00}}' \
  "$EVENT_ID" "$TYPE" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$ORDER_ID")
SIGNATURE=$(printf '%s' "$EVENT_ID.$TIMESTAMP.$BODY" | openssl dgst -sha256 -hmac "$SECRET" -binary | base64)

curl -sS -i -X POST "$URL" \
  -H "content-type: application/json" \
  -H "webhook-id: $EVENT_ID" \
  -H "webhook-timestamp: $TIMESTAMP" \
  -H "webhook-signature: v1,$SIGNATURE" \
  -d "$BODY"
echo
echo "event id: $EVENT_ID"
