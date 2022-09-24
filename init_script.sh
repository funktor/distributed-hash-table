#!/bin/bash
rm -rf registry.txt commit-log-*.txt
start "Registry Service" python "registry_service.py" "1000" "-1" "127.0.0.1" "5001"
start "HashTable Service 1" python "hashtable_service.py" "100" "1" "127.0.0.1" "5003"
start "HashTable Service 2" python "hashtable_service.py" "101" "1" "127.0.0.1" "5004"
start "HashTable Service 3" python "hashtable_service.py" "102" "2" "127.0.0.1" "5005"
start "HashTable Service 4" python "hashtable_service.py" "103" "2" "127.0.0.1" "5006"
start "HealthCheck Service" python "healthcheck_service.py" "2000" "-2" "127.0.0.1" "5002"
start "Routing Service" python "route.py" "3000" "-3" "127.0.0.1" "5007"
start "Client Service" python "client.py" "127.0.0.1" "5007"
