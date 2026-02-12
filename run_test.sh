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

# run 0, iteration 0, checkpoint 1: stop at 10 seconds
# run 0, iteration 0, checkpoint 2: stop at 11 seconds
# run 0, iteration 0, checkpoint 3: stop at 12 seconds

# run 0, iteration 1, checkpoint 1: stop at 13 seconds
# run 0, iteration 1, checkpoint 2: stop at 14 seconds
# run 0, iteration 1, checkpoint 3: stop at 15 seconds

# run 1, iteration 0, checkpoint 1: stop at 22 seconds
# run 1, iteration 0, checkpoint 2: stop at 23 seconds
# run 1, iteration 0, checkpoint 3: stop at 24 seconds

# run 1, iteration 1, checkpoint 1: stop at 25 seconds
# run 1, iteration 2, checkpoint 2: stop at 26 seconds
# run 1, iteration 3, checkpoint 3: stop at 27 seconds
sleep 14
if [ -n "$PID" ] && kill -0 $PID 2>/dev/null; then
    echo "Stopping simple_stream.py (PID: $PID)..."
    kill -TERM $PID 2>/dev/null
    wait $PID 2>/dev/null
fi

echo "Shutting down simple_stream.py"

echo "All processes terminated."
exit 0