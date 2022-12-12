#!/bin/bash
rm -rf commit-log*
start "hash table 1" python "raft.py" "127.0.0.1" "5001" "[['127.0.0.1:5001', '127.0.0.1:5002', '127.0.0.1:5003']]"
start "hash table 2" python "raft.py" "127.0.0.1" "5002" "[['127.0.0.1:5001', '127.0.0.1:5002', '127.0.0.1:5003']]"
start "hash table 3" python "raft.py" "127.0.0.1" "5003" "[['127.0.0.1:5001', '127.0.0.1:5002', '127.0.0.1:5003']]"
start "client service" python "client2.py" "127.0.0.1" "5001"