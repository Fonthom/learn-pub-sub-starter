#!/bin/bash
# Run the war scenario
set -m

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

rm -f /tmp/war_fifo_w /tmp/war_fifo_n /tmp/server_out /tmp/w_out /tmp/n_out
mkfifo /tmp/war_fifo_w /tmp/war_fifo_n

echo "=== Starting Peril server ==="
./server &>/tmp/server_out &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"
sleep 3

# Open write ends of FIFOs in background subshells (will block until read end opened)
# Then start clients which open read ends
echo "=== Starting washington client ==="
(exec 7>/tmp/war_fifo_w; sleep 9999) &  # blocks until read end is opened
SLEEP_W_PID=$!
sleep 0.2
./client </tmp/war_fifo_w &>/tmp/w_out &
CLIENT_W_PID=$!

echo "=== Starting napoleon client ==="
(exec 8>/tmp/war_fifo_n; sleep 9999) &  # blocks until read end is opened
SLEEP_N_PID=$!
sleep 0.2
./client </tmp/war_fifo_n &>/tmp/n_out &
CLIENT_N_PID=$!

sleep 2

# Now write commands to FIFOs. Write to the FIFO directly.
echo "=== Sending washington username ==="
echo "washington" >/tmp/war_fifo_w
sleep 2

echo "=== Sending napoleon username ==="
echo "napoleon" >/tmp/war_fifo_n
sleep 2

echo "=== washington spawns ==="
echo "spawn americas infantry" >/tmp/war_fifo_w
sleep 2

echo "=== napoleon spawns ==="
echo "spawn europe cavalry" >/tmp/war_fifo_n
sleep 2

echo "=== washington moves ==="
echo "move europe 1" >/tmp/war_fifo_w
sleep 3

echo ""
echo "=== Checking output ==="
echo "--- washington (last 15 lines) ---"
tail -15 /tmp/w_out 2>/dev/null
echo ""
echo "--- napoleon (last 15 lines) ---"
tail -15 /tmp/n_out 2>/dev/null

echo ""
echo "Server PID: $SERVER_PID"
echo "Washington PID: $CLIENT_W_PID"
echo "Napoleon PID: $CLIENT_N_PID"
echo "Sleep Washington PID: $SLEEP_W_PID"
echo "Sleep Napoleon PID: $SLEEP_N_PID"
