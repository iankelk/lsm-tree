#!/bin/bash

# Set the number of clients
num_clients=5

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
