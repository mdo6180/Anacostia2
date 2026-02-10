#!/bin/bash

cleanup() {
    if [ -n "$PID" ] && kill -0 $PID 2>/dev/null; then
        echo "Stopping simple_stream.py (PID: $PID)..."
        kill -TERM $PID 2>/dev/null
        wait $PID 2>/dev/null
    fi
}

echo "Running simple_stream.py"
python3 simple_stream.py &
PID=$!

# Give the server time to start
sleep 2

# Verify leaf server started successfully
if ! kill -0 $PID 2>/dev/null; then
    echo "Error: simple_stream.py failed to start. Check ./testing_artifacts/anacostia.log for details."
    exit 1
fi

# Create test data
echo "Creating test data..."
python3 stream_test.py &
STREAM_TEST_PID=$!

sleep 25
if [ -n "$PID" ] && kill -0 $PID 2>/dev/null; then
    echo "Stopping simple_stream.py (PID: $PID)..."
    kill -TERM $PID 2>/dev/null
    wait $PID 2>/dev/null
fi

echo "Shutting down simple_stream.py"

echo "Restarting simple_stream.py"
python3 simple_stream.py -r &
PID=$!

sleep 20