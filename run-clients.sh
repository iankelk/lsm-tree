#!/bin/bash
# Shell script to run multiple concurrent clients
# Change the "milgets" string below to work with
# series of similar workloads.
# Set the number of clients (default to 10)
if [ $# -eq 1 ]; then
    num_clients=$1
else
    num_clients=10
fi

# Launch clients with workloads
for i in $(seq 1 $num_clients); do
    echo "Launching client with milgets$i"
    (
    # Send the workload to the client's stdin
    echo "b milgets$i" | ./bin/client
    ) &
done

# Wait for all clients to finish
wait
