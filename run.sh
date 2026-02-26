#!/usr/bin/env bash
set -euo pipefail

# House Hunter launcher
# Usage:
#   ./run.sh                    — web UI (default)
#   ./run.sh cli "Location"     — CLI mode
#   ./run.sh web                — web UI explicitly
#
# Environment:
#   LLAMA_PORT      — llama-server port (default: 8081)
#   LLAMA_MODEL     — path to GGUF model file (auto-starts llama-server if set)
#   HH_PORT         — web UI port (default: 8181)
#   HH_HOST         — web UI host (default: 127.0.0.1)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

LLAMA_PORT="${LLAMA_PORT:-8081}"
LLAMA_MODEL="${LLAMA_MODEL:-}"
HH_PORT="${HH_PORT:-8181}"
HH_HOST="${HH_HOST:-127.0.0.1}"

LLAMA_PID=""

cleanup() {
    if [[ -n "$LLAMA_PID" ]]; then
        echo "Stopping llama-server (pid $LLAMA_PID)..."
        kill "$LLAMA_PID" 2>/dev/null || true
        wait "$LLAMA_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# Check if llama-server is already running on the port
check_llama() {
    curl -sf "http://localhost:${LLAMA_PORT}/v1/models" >/dev/null 2>&1
}

# Start llama-server if LLAMA_MODEL is set and server isn't already running
start_llama() {
    if check_llama; then
        echo "llama-server already running on port $LLAMA_PORT"
        return
    fi

    if [[ -z "$LLAMA_MODEL" ]]; then
        echo "No llama-server on port $LLAMA_PORT and LLAMA_MODEL not set."
        echo "Either:"
        echo "  1. Start llama-server yourself"
        echo "  2. Set LLAMA_MODEL=/path/to/model.gguf to auto-start"
        echo "  3. Set HOUSE_HUNTER_MODEL and provider API key for cloud LLM"
        exit 1
    fi

    if ! command -v llama-server >/dev/null 2>&1; then
        echo "Error: llama-server not found in PATH"
        exit 1
    fi

    if [[ ! -f "$LLAMA_MODEL" ]]; then
        echo "Error: model file not found: $LLAMA_MODEL"
        exit 1
    fi

    echo "Starting llama-server on port $LLAMA_PORT..."
    llama-server \
        --model "$LLAMA_MODEL" \
        --port "$LLAMA_PORT" \
        --ctx-size 8192 \
        --n-gpu-layers 99 \
        >/dev/null 2>&1 &
    LLAMA_PID=$!

    # Wait for server to be ready
    echo -n "Waiting for llama-server"
    for i in $(seq 1 30); do
        if check_llama; then
            echo " ready!"
            return
        fi
        echo -n "."
        sleep 1
    done
    echo " timeout!"
    echo "llama-server failed to start. Check model path and GPU availability."
    exit 1
}

# Ensure dependencies
if [[ ! -d ".venv" ]]; then
    echo "Installing dependencies..."
    uv sync
fi

# Determine mode
MODE="${1:-web}"
shift 2>/dev/null || true

# Set up local LLM if no cloud model configured
if [[ -z "${HOUSE_HUNTER_MODEL:-}" ]]; then
    start_llama
    export HOUSE_HUNTER_API_BASE="http://localhost:${LLAMA_PORT}/v1"
fi

case "$MODE" in
    web)
        echo "Starting House Hunter web UI at http://${HH_HOST}:${HH_PORT}"
        exec uv run python -m house_hunter.main --web --host "$HH_HOST" --port "$HH_PORT" "$@"
        ;;
    cli)
        exec uv run python -m house_hunter.main "$@"
        ;;
    *)
        # Assume it's a location for CLI mode
        exec uv run python -m house_hunter.main "$MODE" "$@"
        ;;
esac
