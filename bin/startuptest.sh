#!/bin/bash
# Runs heroku local for 10 seconds, captures output, kills process group, and prints output.
# NOTE: Output is printed to stdout, but also kept in heroku_local_output.log
# Use this primarily to check initial startup status.

PORT=5006

# Check if port is in use and kill the process if it is
echo "Checking if port $PORT is in use..."
EXISTING_PID=$(lsof -i :$PORT -t)
if [ -n "$EXISTING_PID" ]; then
  echo "Port $PORT is in use by PID $EXISTING_PID. Attempting to kill..."
  kill -9 $EXISTING_PID || echo "Failed to kill PID $EXISTING_PID (maybe already gone?)"
  sleep 1 # Give a moment for the port to free up
  echo "Port $PORT should be free now."
else
  echo "Port $PORT is free."
fi

echo "Starting heroku local for 10 seconds..."
heroku local > heroku_local_output.log 2>&1 &
PID=$!
echo "heroku local running in background (PID: $PID). Waiting 10 seconds..."
sleep 10
echo "Attempting to kill child processes of PID $PID (using pkill -P)..."
# Use pkill to kill children, then kill the parent just in case
pkill -P $PID || echo "pkill failed or no children found for $PID"
kill $PID || echo "Parent kill $PID failed or process already gone"
wait $PID 2>/dev/null || true # Wait for the original parent process
echo "Process group terminated. Captured output:"
echo "------------------------------------"
cat heroku_local_output.log
echo "------------------------------------"
echo "Log content also available in heroku_local_output.log" 