#!/usr/bin/env bash
#
# Production runner script for coordinated agave-validator and jito-builder execution
#

set -eo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. || exit 1; pwd)"

# Configuration defaults (can be overridden via environment variables or CLI flags)
LEDGER_DIR="${LEDGER_DIR:-/home/solana/ledger}"
IDENTITY_KEYPAIR="${IDENTITY_KEYPAIR:-/home/solana/validator-keypair.json}"
VOTE_KEYPAIR="${VOTE_KEYPAIR:-/home/solana/vote-account-keypair.json}"
BAM_REGISTRY_URL="${BAM_NODE_REGISTRY_URL:-https://bam.jito.network/nodes.json}"
BAM_URL="${BAM_URL:-}"
COMMISSION_BPS="${COMMISSION_BPS:-800}"
NUM_WORKERS="${NUM_WORKERS:-8}"
LOG_DIR="${LOG_DIR:-/home/solana/logs}"
EXTRA_VALIDATOR_ARGS=()

usage() {
  cat <<EOF
Usage: $0 [OPTIONS] [-- <EXTRA_VALIDATOR_ARGS>...]

Runs agave-validator with --enable-scheduler-bindings and orchestrates jito-builder alongside it.

Options:
  --ledger DIR                  Ledger directory (default: $LEDGER_DIR)
  --identity PATH               Validator identity keypair (default: $IDENTITY_KEYPAIR)
  --vote-account PATH           Vote account keypair (default: $VOTE_KEYPAIR)
  --bam-registry-url URL        BAM discovery registry endpoint (default: $BAM_REGISTRY_URL)
  --bam-url URL                 Direct BAM node endpoint (overrides registry if set)
  --commission-bps BPS          Jito BAM commission BPS (default: $COMMISSION_BPS)
  --num-workers COUNT           Scheduler worker threads (default: $NUM_WORKERS)
  --log-dir DIR                 Log directory (default: $LOG_DIR)
  -h, --help                    Show this help message

All arguments after '--' are forwarded directly to agave-validator.
EOF
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ledger)
      LEDGER_DIR="$2"
      shift 2
      ;;
    --identity)
      IDENTITY_KEYPAIR="$2"
      shift 2
      ;;
    --vote-account)
      VOTE_KEYPAIR="$2"
      shift 2
      ;;
    --bam-registry-url)
      BAM_REGISTRY_URL="$2"
      shift 2
      ;;
    --bam-url)
      BAM_URL="$2"
      shift 2
      ;;
    --commission-bps)
      COMMISSION_BPS="$2"
      shift 2
      ;;
    --num-workers)
      NUM_WORKERS="$2"
      shift 2
      ;;
    --log-dir)
      LOG_DIR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      ;;
    --)
      shift
      EXTRA_VALIDATOR_ARGS+=("$@")
      break
      ;;
    *)
      EXTRA_VALIDATOR_ARGS+=("$1")
      shift
      ;;
  esac
done

mkdir -p "$LOG_DIR"
mkdir -p "$LEDGER_DIR"

# 1. System checks
echo "=== Checking system prerequisites ==="
NOFILE_LIMIT=$(ulimit -n)
if [[ "$NOFILE_LIMIT" -lt 1000000 ]]; then
  echo "WARNING: open file limit is $NOFILE_LIMIT (recommended: 1000000). Run 'ulimit -n 1000000'."
fi

# 2. Locate binaries
if command -v agave-validator &>/dev/null; then
  VALIDATOR_BIN="agave-validator"
elif [[ -x "$here/target/release/agave-validator" ]]; then
  VALIDATOR_BIN="$here/target/release/agave-validator"
elif [[ -x "$here/target/debug/agave-validator" ]]; then
  VALIDATOR_BIN="$here/target/debug/agave-validator"
else
  VALIDATOR_BIN="agave-validator"
fi

if command -v jito-builder &>/dev/null; then
  BUILDER_BIN="jito-builder"
elif [[ -x "$here/target/release/jito-builder" ]]; then
  BUILDER_BIN="$here/target/release/jito-builder"
elif [[ -x "$here/target/debug/jito-builder" ]]; then
  BUILDER_BIN="$here/target/debug/jito-builder"
else
  BUILDER_BIN="jito-builder"
fi

echo "Using agave-validator: $VALIDATOR_BIN"
echo "Using jito-builder:     $BUILDER_BIN"

# 3. Setup signal handlers
VALIDATOR_PID=""
BUILDER_PID=""

cleanup() {
  echo ""
  echo "=== Received termination signal, shutting down services ==="
  if [[ -n "$BUILDER_PID" ]] && kill -0 "$BUILDER_PID" 2>/dev/null; then
    echo "Stopping jito-builder (PID: $BUILDER_PID)..."
    kill -TERM "$BUILDER_PID" 2>/dev/null || true
    wait "$BUILDER_PID" 2>/dev/null || true
  fi

  if [[ -n "$VALIDATOR_PID" ]] && kill -0 "$VALIDATOR_PID" 2>/dev/null; then
    echo "Stopping agave-validator (PID: $VALIDATOR_PID)..."
    kill -INT "$VALIDATOR_PID" 2>/dev/null || true
    wait "$VALIDATOR_PID" 2>/dev/null || true
  fi
  echo "Services stopped cleanly."
  exit 0
}

trap cleanup INT TERM HUP

# 4. Start agave-validator
VALIDATOR_CMD=(
  "$VALIDATOR_BIN"
  --ledger "$LEDGER_DIR"
  --identity "$IDENTITY_KEYPAIR"
  --vote-account "$VOTE_KEYPAIR"
  --enable-scheduler-bindings
  "${EXTRA_VALIDATOR_ARGS[@]}"
)

echo "Starting agave-validator..."
echo "${VALIDATOR_CMD[*]}"
"${VALIDATOR_CMD[@]}" > >(tee -a "$LOG_DIR/validator.log") 2>&1 &
VALIDATOR_PID=$!
echo "agave-validator started with PID: $VALIDATOR_PID"

# 5. Wait for IPC sockets
IPC_SOCKET="$LEDGER_DIR/scheduler_bindings.ipc"
ADMIN_SOCKET="$LEDGER_DIR/admin.rpc"

echo "Waiting for validator IPC sockets ($IPC_SOCKET, $ADMIN_SOCKET)..."
SOCKET_READY=0
for i in {1..60}; do
  if ! kill -0 "$VALIDATOR_PID" 2>/dev/null; then
    echo "ERROR: agave-validator exited prematurely. Check $LOG_DIR/validator.log for details."
    exit 1
  fi
  if [[ -S "$IPC_SOCKET" && -S "$ADMIN_SOCKET" ]]; then
    SOCKET_READY=1
    break
  fi
  sleep 1
done

if [[ "$SOCKET_READY" -ne 1 ]]; then
  echo "ERROR: Timed out waiting for scheduler_bindings.ipc and admin.rpc sockets."
  kill -INT "$VALIDATOR_PID" 2>/dev/null || true
  exit 1
fi
echo "Validator sockets detected successfully."

# 6. Start jito-builder
BUILDER_CMD=(
  "$BUILDER_BIN"
  --ledger "$LEDGER_DIR"
  --commission-bps "$COMMISSION_BPS"
  --num-workers "$NUM_WORKERS"
)

if [[ -n "$BAM_URL" ]]; then
  BUILDER_CMD+=(--bam-url "$BAM_URL")
else
  BUILDER_CMD+=(--bam-node-registry-url "$BAM_REGISTRY_URL")
fi

echo "Starting jito-builder daemon..."
echo "${BUILDER_CMD[*]}"
"${BUILDER_CMD[@]}" > >(tee -a "$LOG_DIR/jito-builder.log") 2>&1 &
BUILDER_PID=$!
echo "jito-builder daemon started with PID: $BUILDER_PID"

# 7. Monitor processes
while true; do
  if ! kill -0 "$VALIDATOR_PID" 2>/dev/null; then
    echo "ERROR: agave-validator process died."
    cleanup
  fi
  if ! kill -0 "$BUILDER_PID" 2>/dev/null; then
    echo "WARNING: jito-builder process died. Restarting in 5s..."
    sleep 5
    "${BUILDER_CMD[@]}" > >(tee -a "$LOG_DIR/jito-builder.log") 2>&1 &
    BUILDER_PID=$!
    echo "Restarted jito-builder with PID: $BUILDER_PID"
  fi
  sleep 2
done
