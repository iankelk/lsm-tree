#!/bin/bash

# Set the number of clients
num_clients=10

# Launch clients with workloads
for i in $(seq 1 $num_clients); do
    echo "Launching client with workload$i"
    (./bin/client << EOF
b milgets$i
EOF
    ) &
done

# Wait for all clients to finish
wait
