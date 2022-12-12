from random import randint, random
import sys, os, re, socket, select

from requests import delete
from hashtable import HashTable
from threading import Thread, Lock
import mmh3, time
from queue import Queue
from random import shuffle
from commit_log import CommitLog
import tqdm
from pathlib import Path
from consistent_hashing import ConsistentHashing
import shutil, utils
import traceback


class Raft:
    def __init__(self, ip, port, partitions):
        self.ip = ip
        self.port = port
        self.ht = HashTable()
        self.commit_log = CommitLog(file=f"commit-log-{self.ip}-{self.port}.txt")
        self.partitions = eval(partitions)
        self.conns = [[None]*len(self.partitions[i]) for i in range(len(self.partitions))]
        self.cluster_index = -1
        self.server_index = -1
        
        self.cluster_lock = Lock()
        self.socket_locks = [[Lock() for j in range(len(self.partitions[i]))] for i in range(len(self.partitions))]
        
        # Initialize commit log file
        commit_logfile = Path(self.commit_log.file)
        commit_logfile.touch(exist_ok=True)

        for i in range(len(self.partitions)):
            cluster = self.partitions[i]
            
            for j in range(len(cluster)):
                ip, port = cluster[j].split(':')
                port = int(port)
                
                if (ip, port) == (self.ip, self.port):
                    self.cluster_index = i
                    self.server_index = j
                    
                else: 
                    # 3rd element is the socket object
                    self.conns[i][j] = [ip, port, None]
        
        self.current_term = 1
        self.voted_for = -1
        self.votes = set()
        
        u = len(self.partitions[self.cluster_index])
        
        self.state = 'FOLLOWER'
        self.leader_id = -1
        self.commit_index = 0
        self.next_indices = [0]*u
        self.match_indices = [-1]*u
        self.election_period_ms = randint(1000, 2000)
        self.rpc_period_ms = 3000
        self.election_timeout = -1
        self.rpc_timeout = [-1]*u
        self.state_lock = Lock()
        self.election_timeout_lock = Lock()
        self.term_lock = Lock()
        self.vote_lock = Lock()
        self.next_indices_lock = Lock()
        
        utils.run_thread(fn=self.wait_for_servers, args=())
        
        print("Ready....")
        
    def wait_for_servers(self):
        print("Waiting for servers to be ready...")
        
        threads = []
        for j in range(len(self.partitions[self.cluster_index])):
            if j != self.server_index:
               t =  utils.run_thread(fn=utils.wait_for_server_startup, 
                                     args=(self.conns[self.cluster_index], j, ))
               threads += [t]
        
        for t in threads:
            t.join()
            
        utils.run_thread(fn=self.on_election_timeout, args=())
        utils.run_thread(fn=self.leader_send_append_entries, args=())
        
    def set_election_timeout(self):
        with self.election_timeout_lock:
            self.election_timeout = time.time() + randint(self.election_period_ms, 2*self.election_period_ms)/1000.0
    
    def get_election_timeout(self):
        with self.election_timeout_lock:
            return self.election_timeout
        
    def set_state(self, state):
        with self.state_lock:
            self.state = state
            return self.state
            
    def get_state(self):
        with self.state_lock:
            return self.state
        
    def increment_term(self):
        with self.term_lock:
            self.current_term += 1
            return self.current_term
            
    def set_term(self, term):
        with self.term_lock:
            self.current_term = term
            return self.current_term
    
    def get_term(self):
        with self.term_lock:
            return self.current_term
        
    def set_vote(self, vote):
        with self.vote_lock:
            self.voted_for = vote
            return self.voted_for
                
    def set_self_vote(self, vote):
        with self.vote_lock:
            self.votes.add(vote)
            return self.votes
        
    def get_vote(self):
        with self.vote_lock:
            return self.voted_for
        
    def get_all_votes(self):
        with self.vote_lock:
            return self.votes
        
    def set_next_indices(self, i, v):
        with self.next_indices_lock:
            self.next_indices[i] = v
            return self.next_indices[i]
    
    def get_next_indices_j(self, j):
        with self.next_indices_lock:
            return self.next_indices[j]
    
    def on_election_timeout(self):
        while True:
            state = self.get_state()
            if time.time() > self.get_election_timeout() and \
                (state == 'FOLLOWER' or state == 'CANDIDATE'):
                
                print("Timeout....")   
                self.start_election() 
                        
    def start_election(self):
        print("Starting election...")
        
        self.set_election_timeout()                
        self.set_state('CANDIDATE')
        self.increment_term()
        self.set_vote(self.server_index)
        self.set_self_vote(self.server_index)
        
        threads = []
        for j in range(len(self.partitions[self.cluster_index])):
            if j != self.server_index:
               t =  utils.run_thread(fn=self.request_vote, args=(j,))
               threads += [t]
        
        for t in threads:
            t.join()
        
        return True
    
    def request_vote(self, server):
        current_term = self.get_term()
        
        last_term = self.commit_log.last_term
        last_indx = self.commit_log.last_index
        
        while True:
            print(f"Requesting vote from {server}...")
            state = self.get_state()
            
            if time.time() <= self.get_election_timeout() and \
                (state == 'FOLLOWER' or state == 'CANDIDATE'):
                    
                resp = \
                    utils.send_and_recv_no_retry(f"VOTE-REQ {self.server_index} {current_term} {last_term} {last_indx}", 
                                                 self.conns[self.cluster_index], 
                                                 self.socket_locks[self.cluster_index], 
                                                 server, 
                                                 timeout=self.rpc_period_ms/1000.0)
                
                vote_rep = re.match('^VOTE-REP ([0-9]+) ([0-9\-]+) ([0-9\-]+)$', resp)
                
                if vote_rep:
                    server, curr_term, voted_for = vote_rep.groups()
                    server = int(server)
                    curr_term = int(curr_term)
                    voted_for = int(voted_for)
                    
                    self.process_vote_reply(server, curr_term, voted_for)
                    break
            else:
                break
    
    def process_vote_request(self, server, term, last_term, last_index):
        print(f"Processing vote request from {server} {term}...")
        
        current_term = self.get_term()
        voted_for = self.get_vote()
        
        if term > current_term:
            current_term, state, voted_for = self.step_down(term)
        else:
            self_last_term = self.commit_log.last_term
            self_last_indx = self.commit_log.last_index
            
            if term == current_term \
                and (voted_for == server or voted_for == -1) \
                and ((last_index == -1 and self_last_indx == -1) \
                    or last_term > self_last_term \
                    or (last_term == self_last_term and last_index > self_last_indx)):
                
                voted_for = self.set_vote(server)    
                self.set_election_timeout()
        
        return f"VOTE-REP {self.server_index} {current_term} {voted_for}"
        
    def process_vote_reply(self, server, term, voted_for):
        print(f"Processing vote reply from {server} {term}...")
        
        current_term = self.get_term()
        state = self.get_state()
        votes = self.get_all_votes()
        
        if term > current_term:
            current_term, state, voted_for = self.step_down(term)
        
        elif term == current_term and state == 'CANDIDATE':
            if voted_for == self.server_index:
                votes = self.set_self_vote(server)
            
            if len(votes) > len(self.partitions[self.cluster_index])/2.0:
                state = self.set_state('LEADER')
                self.leader_id = self.server_index
                print(f"{self.cluster_index}-{self.server_index} became leader")
                print(f"{votes}-{current_term}")
        
        return True
                
    def step_down(self, term):
        print(f"Stepping down...")
        term = self.set_term(term)
        state = self.set_state('FOLLOWER')
        voted_for = self.set_vote(-1)
        self.set_election_timeout() 
        
        return term, state, voted_for
    
    def leader_send_append_entries(self):
        print(f"Sending append entries from leader...")
        
        while True:
            self.append_entries()
                    
    def append_entries(self):
        state = self.get_state()
            
        if state == 'LEADER':
            threads = []
            for j in range(len(self.partitions[self.cluster_index])):
                if j != self.server_index:
                    t = utils.run_thread(fn=self.send_append_entries_request, args=(j,))
                    threads += [t]
            
            for t in threads:
                t.join()
        
        return True
    
    def send_append_entries_request(self, server):
        print(f"Sending append entries to {server}...")
        
        current_term = self.get_term()
        size = self.commit_log.last_index+1
        
        with self.next_indices_lock:
            self.next_indices[server] = min(self.next_indices[server], size)
            
        next_indices_server = self.get_next_indices_j(server)
        
        prev_idx = next_indices_server-1
        log_slice = self.commit_log.read_logs_start_end(prev_idx)
        
        if prev_idx == -1:
            prev_term = 0
        else:
            prev_term = log_slice[0][0]
            log_slice = log_slice[1:] if len(log_slice) > 1 else []
        
        msg = f"APPEND-REQ {self.server_index} {current_term} {prev_idx} {prev_term} {str(log_slice)} {self.commit_index}"
        
        while True:
            state = self.get_state()
            
            if state == 'LEADER':
                resp = \
                    utils.send_and_recv_no_retry(msg, 
                                                self.conns[self.cluster_index], 
                                                self.socket_locks[self.cluster_index], 
                                                server, 
                                                timeout=self.rpc_period_ms/1000.0)  
                
                append_rep = re.match('^APPEND-REP ([0-9]+) ([0-9\-]+) ([0-9]+) ([0-9\-]+)$', resp)
                
                if append_rep:
                    server, curr_term, flag, index = append_rep.groups()
                    server = int(server)
                    curr_term = int(curr_term)
                    flag = int(flag)
                    success = True if flag == 1 else False
                    index = int(index)
                    
                    self.process_append_reply(server, curr_term, success, index)
                    break
            else:
                break
        
    def process_append_requests(self, server, term, prev_idx, prev_term, logs, commit_index):
        print(f"Processing append request from {server} {term}...")
        
        current_term = self.get_term()
        flag, index = 0, 0
        self.leader_id = server
        
        if term > current_term:
            current_term, state, voted_for = self.step_down(term)
            
        elif term >= current_term:
            self_logs = self.commit_log.read_logs_start_end(prev_idx, prev_idx) if prev_idx != -1 else []
            success = prev_idx == -1 or (len(self_logs) > 0 and self_logs[0][0] == prev_term)
            
            if success:
                index = self.store_entries(prev_idx, logs)
                
            self.set_election_timeout()
            flag = 1 if success else 0
        
        return f"APPEND-REP {self.server_index} {current_term} {flag} {index}"
            
    def process_append_reply(self, server, term, success, index):
        print(f"Processing append reply from {server} {term}...")
        
        current_term = self.get_term()
        state = self.get_state()
        
        if term > current_term:
            current_term, state, voted_for = self.step_down(term)
            
        elif state == 'LEADER' and term == current_term:
            if success:
                self.set_next_indices(server, index+1)
            else:
                with self.next_indices_lock:
                    self.next_indices[server] = max(0, self.next_indices[server]-1)
        
                self.send_append_entries_request(server) 
                
    def store_entries(self, prev_idx, leader_logs):
        current_term = self.get_term()
        
        commands = [f"{leader_logs[i][1]}" for i in range(len(leader_logs))]
        index = self.commit_log.log_replace(current_term, commands, prev_idx+1)
        self.commit_index = index
        
        for command in commands:
            set_ht = re.match('^SET ([^\s]+) ([^\s]+) ([0-9]+)$', command)
            
            if set_ht:
                key, value, req_id = set_ht.groups()
                req_id = int(req_id)
                self.ht.set(key=key, value=value, req_id=req_id)
        
        return index
        
    def handle_commands(self, msg, conn):
        set_ht = re.match('^SET ([^\s]+) ([^\s]+) ([0-9]+)$', msg)
        get_ht = re.match('^GET ([^\s]+) ([0-9]+)$', msg)
        vote_req = re.match('^VOTE-REQ ([0-9]+) ([0-9\-]+) ([0-9\-]+) ([0-9\-]+)$', msg)
        append_req = re.match('^APPEND-REQ ([0-9]+) ([0-9\-]+) ([0-9\-]+) ([0-9\-]+) (\[.*?\]) ([0-9\-]+)$', msg)

        if set_ht:
            output = "ko"
            
            try:
                key, value, req_id = set_ht.groups()
                req_id = int(req_id)
                node = mmh3.hash(key, signed=False) % len(self.partitions)
                print(node)
                print(self.leader_id)
                
                if self.cluster_index == node:
                    # The key is intended for current cluster
                    
                    state = self.get_state()
                    current_term = self.get_term()
                    
                    if state == 'LEADER':
                        # Replicate if this is leader server and req_id is the latest one corresponding to key
                        self.commit_log.log(current_term, msg)
                        replicated = self.append_entries()
                        
                        if replicated:
                            self.ht.set(key=key, value=value, req_id=req_id)
                            output = "ok"
                    else:
                        output = utils.send_and_recv(msg, self.conns[node], self.socket_locks[node], self.leader_id)
                else:
                    # Forward to relevant cluster if key is not intended for this cluster
                    output = utils.send_and_recv(msg, self.conns[node], self.socket_locks[node], 0)
                    if output is None:
                        output = "ko"
                        
            except Exception as e:
                traceback.print_exc()
   
        elif get_ht:
            output = "ko"
            
            try:
                key, _ = get_ht.groups()
                node = mmh3.hash(key, signed=False) % len(self.partitions)
                
                if self.cluster_index == node:
                    # The key is intended for current cluster
                    output = self.ht.get_value(key=key)
                    if output:
                        output = str(output)
                        
                else:
                    # Forward the get request to a random node in the correct cluster
                    indices = list(range(len(self.partitions[node])))
                    
                    shuffle(indices)
                    # Loop over multiple indices because some replica might be unresponsive and times out
                    for j in indices:
                        output = utils.send_and_recv(msg, self.conns[node], self.socket_locks[node], j, timeout=self.rpc_period_ms/1000.0)
                        if output:
                            break
                    
                if output is None:
                    output = 'Error: Non existent key'
                    
            except Exception as e:
                traceback.print_exc()
        
        elif vote_req:
            try:
                server, curr_term, last_term, last_indx = vote_req.groups()
                server = int(server)
                curr_term = int(curr_term)
                last_term = int(last_term)
                last_indx = int(last_indx)
                
                output = self.process_vote_request(server, curr_term, last_term, last_indx)
                
            except Exception as e:
                traceback.print_exc()
                
        elif append_req:
            try:
                server, curr_term, prev_idx, prev_term, logs, commit_index = append_req.groups()
                server = int(server)
                curr_term = int(curr_term)
                prev_idx = int(prev_idx)
                prev_term = int(prev_term)
                logs = eval(logs)
                commit_index = int(commit_index)
                
                output = self.process_append_requests(server, curr_term, prev_idx, prev_term, logs, commit_index)
                
            except Exception as e:
                traceback.print_exc() 
                    
        else:
            print("Hello1 - " + msg + " - Hello2")
            output = "Error: Invalid command"
        
        return output
    
    def process_request(self, conn):
        while True:
            try:
                msg = conn.recv(2048).decode()
                print(f"{msg} received")
                output = self.handle_commands(msg, conn)
                conn.send(output.encode())
                
            except Exception as e:
                traceback.print_exc()
                print("Error processing message from client")
                conn.close()
                break
    
    def listen_to_clients(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('0.0.0.0', int(self.port)))
        sock.listen(50)
    
        while True:
            try:
                client_socket, client_address = sock.accept()

                print(f"Connected to new client at address {client_address}")
                my_thread = Thread(target=self.process_request, args=(client_socket,))
                my_thread.daemon = True
                my_thread.start()
                
            except:
                print("Error accepting connection...")

if __name__ == '__main__':
    ip_address = str(sys.argv[1])
    port = int(sys.argv[2])
    partitions = str(sys.argv[3])
    
    dht = Raft(ip=ip_address, port=port, partitions=partitions)
    dht.listen_to_clients()