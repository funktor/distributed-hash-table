#!/bin/bash
rm -rf registry.txt commit-log-*.txt
# start "Registry Service" python "registry_service.py" "1000" "-1" "127.0.0.1" "5001"
# start "HealthCheck Service" python "healthcheck_service.py" "2000" "-2" "127.0.0.1" "5002"
# start "Load Balancer" python "consistent_hashing_service.py" "2000" "-2" "127.0.0.1" "5002"
# start "HashTable Service 1" python "hashtable_service.py" "100" "1" "127.0.0.1" "5003"
# start "HashTable Service 2" python "hashtable_service.py" "101" "1" "127.0.0.1" "5004"
# start "HashTable Service 3" python "hashtable_service.py" "102" "2" "127.0.0.1" "5005"
# start "HashTable Service 4" python "hashtable_service.py" "103" "2" "127.0.0.1" "5006"
# start "HealthCheck Service" python "healthcheck_service.py" "2000" "-2" "127.0.0.1" "5002"
# start "Routing Service" python "route.py" "3000" "-3" "127.0.0.1" "5007"
# start "Client Service" python "client.py" "127.0.0.1" "5007"

python create_cluster.py "1" "registry_service.py" "['127.0.0.1:5001']"
python create_cluster.py "2" "healthcheck_service.py" "['127.0.0.1:5002']"
python create_cluster.py "3" "consistent_hashing_service.py" "['127.0.0.1:5003', '127.0.0.1:5004']"
python create_cluster.py "4" "hashtable_service.py" "['127.0.0.1:5005', '127.0.0.1:5006']"
python create_cluster.py "5" "hashtable_service.py" "['127.0.0.1:5007', '127.0.0.1:5008']"
start "Client Service" python "client.py" "127.0.0.1" "5007"
