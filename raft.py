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
from queue import Queue


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
        self.leader_id_lock = Lock()
        self.commit_index_lock = Lock()
        self.election_lock = Lock()
        
        utils.run_thread(fn=self.wait_for_servers, args=())
        
        print("Ready....")
        
    def wait_for_servers(self):
        print("Waiting for servers to be ready...")
        
        # Wait for all servers in the partition to start
        threads = []
        for j in range(len(self.partitions[self.cluster_index])):
            if j != self.server_index:
               t =  utils.run_thread(fn=utils.wait_for_server_startup, 
                                     args=(self.conns[self.cluster_index], j, ))
               threads += [t]
        
        for t in threads:
            t.join()
        
        # Check for election timeout in the background
        utils.run_thread(fn=self.on_election_timeout, args=())
        
        # Sync logs or send heartbeats from leader to all servers in the background
        utils.run_thread(fn=self.leader_send_append_entries, args=())
        
    def set_election_timeout(self, timeout=None):
        # Reset this whenever previous timeout expires and starts a new election
        if timeout:
            self.election_timeout = timeout
        else:
            self.election_timeout = time.time() + randint(self.election_period_ms, 2*self.election_period_ms)/1000.0
    
    def on_election_timeout(self):
        while True:
            if time.time() > self.election_timeout and \
                (self.state == 'FOLLOWER' or self.state == 'CANDIDATE'):
                
                print("Timeout....") 
                self.set_election_timeout() 
                self.start_election() 
                        
    def start_election(self):
        print("Starting election...")
        
        self.state = 'CANDIDATE'
        self.current_term += 1
        self.voted_for = self.server_index
        self.votes.add(self.server_index)
        
        threads = []
        for j in range(len(self.partitions[self.cluster_index])):
            if j != self.server_index:
               t =  utils.run_thread(fn=self.request_vote, args=(j,))
               threads += [t]
        
        for t in threads:
            t.join()
        
        return True
    
    def request_vote(self, server):
        last_index, last_term = self.commit_log.get_last_index_term()
        
        while True:
            print(f"Requesting vote from {server}...")
            
            if time.time() <= self.election_timeout and \
                (self.state == 'FOLLOWER' or self.state == 'CANDIDATE'):
                    
                resp = \
                    utils.send_and_recv_no_retry(f"VOTE-REQ {self.server_index} {self.current_term} {last_term} {last_index}", 
                                                 self.conns[self.cluster_index], 
                                                 self.socket_locks[self.cluster_index], 
                                                 server, 
                                                 timeout=self.rpc_period_ms/1000.0)
                
                if resp:
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
        
        if term > self.current_term:
            self.step_down(term)
        else:
            self_last_index, self_last_term = self.commit_log.get_last_index_term()
            
            if term == self.current_term \
                and (self.voted_for == server or self.voted_for == -1) \
                and ((last_index == -1 and self_last_index == -1) \
                    or last_term > self_last_term \
                    or (last_term == self_last_term and last_index > self_last_index)):
                
                self.voted_for = server
        
        return f"VOTE-REP {self.server_index} {self.current_term} {self.voted_for}"
        
    def process_vote_reply(self, server, term, voted_for):
        print(f"Processing vote reply from {server} {term}...")
        
        if term > self.current_term:
            self.step_down(term)
        
        elif term == self.current_term and self.state == 'CANDIDATE':
            if voted_for == self.server_index:
                self.votes.add(server)
            
            if len(self.votes) > len(self.partitions[self.cluster_index])/2.0:
                self.state = 'LEADER'
                self.leader_id = self.server_index
                print(f"{self.cluster_index}-{self.server_index} became leader")
                print(f"{self.votes}-{self.current_term}")
                
    def step_down(self, term):
        print(f"Stepping down...")
        self.current_term = term
        self.state = 'FOLLOWER'
        self.voted_for = -1
    
    def leader_send_append_entries(self):
        print(f"Sending append entries from leader...")
        
        while True:
            if self.state == 'LEADER':
                self.append_entries()
                last_index, _ = self.commit_log.get_last_index_term()
                self.commit_index = last_index
                    
    def append_entries(self):
        res = Queue()
            
        for j in range(len(self.partitions[self.cluster_index])):
            if j != self.server_index:
                utils.run_thread(fn=self.send_append_entries_request, args=(j,res,))
                
        cnts = 0
    
        while True:
            # Wait for servers to respond
            res.get(block=True)
            cnts += 1
            # Exclude self
            if cnts > (len(self.partitions[self.cluster_index])/2.0)-1:
                return
            
    
    def send_append_entries_request(self, server, res=None):
        print(f"Sending append entries to {server}...")
        
        prev_idx = self.next_indices[server]-1
        log_slice = self.commit_log.read_logs_start_end(prev_idx)
        
        if prev_idx == -1:
            prev_term = 0
        else:
            prev_term = log_slice[0][0]
            log_slice = log_slice[1:] if len(log_slice) > 1 else []
        
        msg = f"APPEND-REQ {self.server_index} {self.current_term} {prev_idx} {prev_term} {str(log_slice)} {self.commit_index}"
        
        while True:
            if self.state == 'LEADER':
                resp = \
                    utils.send_and_recv_no_retry(msg, 
                                                self.conns[self.cluster_index], 
                                                self.socket_locks[self.cluster_index], 
                                                server, 
                                                timeout=self.rpc_period_ms/1000.0)  
                
                if resp:
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
        
        if res:   
            res.put('ok')
        
    def process_append_requests(self, server, term, prev_idx, prev_term, logs, commit_index):
        print(f"Processing append request from {server} {term}...")
        
        flag, index = 0, 0
        
        if term > self.current_term:
            self.step_down()
        
        if term == self.current_term:
            self.leader_id = server
            
            self_logs = self.commit_log.read_logs_start_end(prev_idx, prev_idx) if prev_idx != -1 else []
            success = prev_idx == -1 or (len(self_logs) > 0 and self_logs[0][0] == prev_term)
            
            if success:
                index = self.store_entries(prev_idx, logs)
                
            flag = 1 if success else 0
            self.set_election_timeout()
        
        return f"APPEND-REP {self.server_index} {self.current_term} {flag} {index}"
            
    def process_append_reply(self, server, term, success, index):
        print(f"Processing append reply from {server} {term}...")
        
        if term > self.current_term:
            self.step_down(term)
            
        elif self.state == 'LEADER' and term == self.current_term:
            if success:
                self.next_indices[server] = index+1
            else:
                self.next_indices[server] = max(0, self.next_indices[server]-1)
                self.send_append_entries_request(server)  
                
    def store_entries(self, prev_idx, leader_logs):
        commands = [f"{leader_logs[i][1]}" for i in range(len(leader_logs))]
        last_index, _ = self.commit_log.log_replace(self.current_term, commands, prev_idx+1)
        self.commit_index = last_index
        
        for command in commands:
            self.update_state_machine(command)
        
        return last_index
    
    def update_state_machine(self, command):
        set_ht = re.match('^SET ([^\s]+) ([^\s]+) ([0-9]+)$', command)
            
        if set_ht:
            key, value, req_id = set_ht.groups()
            req_id = int(req_id)
            self.ht.set(key=key, value=value, req_id=req_id)
            
        
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
                
                # Hash based partitioning
                node = mmh3.hash(key, signed=False) % len(self.partitions)
                
                if self.cluster_index == node:
                    # The key is intended for current cluster
                    if self.state == 'LEADER':
                        # Replicate if this is leader server
                        last_index, _ = self.commit_log.log(self.current_term, msg)
                        # Get response from at-least N/2 number of servers
                        
                        while True:
                            if last_index == self.commit_index:
                                break
                        
                        # Set state machine
                        self.ht.set(key=key, value=value, req_id=req_id)
                        output = "ok"
                    else:
                        # If sent to non-leader, then forward to leader
                        # Do not retry here because it might happen that current server becomes leader after sometime
                        # Retry at client/upstream service end
                        output = utils.send_and_recv_no_retry(msg, 
                                                            self.conns[node], 
                                                            self.socket_locks[node], 
                                                            self.leader_id, 
                                                            timeout=10)
                        if output is None:
                            output = 'ko'
                else:
                    # Forward to relevant cluster (1st in partitions config) if key is not intended for this cluster
                    # Retry here because this is different partition
                    output = utils.send_and_recv(msg, 
                                                 self.conns[node], 
                                                 self.socket_locks[node], 
                                                 0)
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
                    if self.state == 'LEADER':
                        output = self.ht.get_value(key=key)
                        if output:
                            output = str(output)
                        else:
                            output = 'Error: Non existent key'
                        
                    else:
                        # If sent to non-leader, then forward to leader
                        # Do not retry here because it might happen that current server becomes leader after sometime
                        # Retry at client/upstream service end
                        output = utils.send_and_recv_no_retry(msg, 
                                                            self.conns[node], 
                                                            self.socket_locks[node], 
                                                            self.leader_id, 
                                                            timeout=10)
                        if output is None:
                            output = 'ko'
                        
                else:
                    # Forward to relevant cluster (1st in partitions config) if key is not intended for this cluster
                    # Retry here because this is different partition
                    output = utils.send_and_recv(msg, 
                                                 self.conns[node], 
                                                 self.socket_locks[node], 
                                                 0)
                    if output is None:
                        output = "ko"
                    
                    
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