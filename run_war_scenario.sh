#!/bin/bash
set -m

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Kill old processes
pkill -9 -f "./server" 2>/dev/null || true
pkill -9 -f "./client" 2>/dev/null || true
pkill -9 -f "sleep 9999" 2>/dev/null || true
sleep 1

# Clean up
rm -f /tmp/war_fifo_w /tmp/war_fifo_n /tmp/s_out /tmp/w_out /tmp/n_out
rm -f game.log
mkfifo /tmp/war_fifo_w /tmp/war_fifo_n

# Delete queues
curl -s -u guest:guest -X DELETE http://localhost:15672/api/queues/%2F/game_logs 2>/dev/null
curl -s -u guest:guest -X DELETE http://localhost:15672/api/queues/%2F/war 2>/dev/null

echo "=== Starting server ==="
nohup ./server &>/tmp/s_out &
SERVER_PID=$!
sleep 3
echo "Server PID=$SERVER_PID"

# Start FIFO keepers and clients
echo "=== Starting washington ==="
nohup bash -c 'exec 7>/tmp/war_fifo_w; sleep 9999' &
SLEEP_W_PID=$!
sleep 0.3
nohup ./client </tmp/war_fifo_w &>/tmp/w_out &
CLIENT_W_PID=$!

echo "=== Starting napoleon ==="
nohup bash -c 'exec 8>/tmp/war_fifo_n; sleep 9999' &
SLEEP_N_PID=$!
sleep 0.3
nohup ./client </tmp/war_fifo_n &>/tmp/n_out &
CLIENT_N_PID=$!

sleep 3

echo "=== Sending usernames ==="
echo "washington" >/tmp/war_fifo_w || exit 1
sleep 2
echo "napoleon" >/tmp/war_fifo_n || exit 1
sleep 2

echo "=== War 1: spawn & move ==="
echo "spawn americas infantry" >/tmp/war_fifo_w || exit 1
sleep 2
echo "spawn europe cavalry" >/tmp/war_fifo_n || exit 1
sleep 2
echo "move europe 1" >/tmp/war_fifo_w || exit 1
sleep 8

echo "=== War 2: spawn & move ==="
echo "spawn americas infantry" >/tmp/war_fifo_w || exit 1
sleep 2
echo "spawn europe infantry" >/tmp/war_fifo_n || exit 1
sleep 2
echo "move europe 1" >/tmp/war_fifo_w || exit 1
sleep 8

echo "=== War 3: spawn & move ==="
echo "spawn americas cavalry" >/tmp/war_fifo_w || exit 1
sleep 2
echo "spawn europe artillery" >/tmp/war_fifo_n || exit 1
sleep 2
echo "move europe 1" >/tmp/war_fifo_w || exit 1
sleep 8

echo ""
echo "=== game.log content ==="
cat game.log 2>/dev/null
echo ""
echo "=== game_logs queue ==="
curl -s -u guest:guest http://localhost:15672/api/queues/%2F/game_logs | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'messages={d.get(\"messages\",0)}')"
echo ""
echo "All done."
