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
        
        self.state = 'FOLLOWER' if len(self.partitions[self.cluster_index]) > 1 else 'LEADER'
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
        self.raft_lock = Lock()
        
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
        
        # set initial election timeout
        self.set_election_timeout()
        
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
            # Check everytime that state is either FOLLOWER or CANDIDATE before sending
            # vote requests.
            
            # The possibilities in this path are:
            # 1. Requestor sends requests, receives replies and becomes leader
            # 2. Requestor sends requests, receives replies and becomes follower again, repeat on election timeout
            with self.raft_lock:
                if time.time() > self.election_timeout and \
                    (self.state == 'FOLLOWER' or self.state == 'CANDIDATE'):
                    
                    print("Timeout....") 
                    self.set_election_timeout() 
                    self.start_election() 
                        
    def start_election(self):
        print("Starting election...")
        
        # At the start of election, set state to CANDIDATE and increment term
        # also vote for self.
        self.state = 'CANDIDATE'
        self.current_term += 1
        self.voted_for = self.server_index
        self.votes.add(self.server_index)
        
        # Send vote requests in parallel
        threads = []
        for j in range(len(self.partitions[self.cluster_index])):
            if j != self.server_index:
               t =  utils.run_thread(fn=self.request_vote, args=(j,))
               threads += [t]
        
        # Wait for completion of request flow
        for t in threads:
            t.join()
        
        return True
    
    def request_vote(self, server):
        # Get last index and term from commit log
        last_index, last_term = self.commit_log.get_last_index_term()
        
        while True:
            # Retry on timeout
            print(f"Requesting vote from {server}...")
            
            # Check if state if still CANDIDATE
            if self.state == 'CANDIDATE' and time.time() < self.election_timeout:
                resp = \
                    utils.send_and_recv_no_retry(f"VOTE-REQ {self.server_index} {self.current_term} {last_term} {last_index}", 
                                                 self.conns[self.cluster_index], 
                                                 self.socket_locks[self.cluster_index], 
                                                 server, 
                                                 timeout=self.rpc_period_ms/1000.0)
                
                # If timeout happens resp returns None, so it won't go inside this condition
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
        
        with self.raft_lock:
            if term > self.current_term:
                # Requestor term is higher hence update
                self.step_down(term)

            # Get last index and term from log
            self_last_index, self_last_term = self.commit_log.get_last_index_term()
            
            # Vote for requestor only if requestor term is equal to self term
            # and self has either voted for no one yet or has voted for same requestor (can happen during failure/timeout and retry)
            # and either both requestor and self have empty logs (starting up)
            # or the last term of requestor is greater 
            # or if they are equal then the last index of requestor should be greater.
            # This is to ensure that only vote for all requestors who have updated logs.
            if term == self.current_term \
                and (self.voted_for == server or self.voted_for == -1) \
                and ((last_index == -1 and self_last_index == -1) \
                    or last_term > self_last_term \
                    or (last_term == self_last_term and last_index > self_last_index)):
                
                self.voted_for = server
            
            return f"VOTE-REP {self.server_index} {self.current_term} {self.voted_for}"
        
    def process_vote_reply(self, server, term, voted_for):
        print(f"Processing vote reply from {server} {term}...")
        
        # It is not possible to have term < self.current_term because during vote request
        # the server will update its term to match requestor term if requestor term is higher
        if term > self.current_term:
            # Requestor term is lower hence update
            self.step_down(term)
        
        if term == self.current_term and self.state == 'CANDIDATE':
            if voted_for == self.server_index:
                self.votes.add(server)
            
            # Convert to leader if received votes from majority
            if len(self.votes) > len(self.partitions[self.cluster_index])/2.0:
                self.state = 'LEADER'
                self.leader_id = self.server_index
                
                # Update state machine i.e. in memory hash map
                for term, command in self.commit_log.read_log():
                    self.update_state_machine(command)
                
                print(f"{self.cluster_index}-{self.server_index} became leader")
                print(f"{self.votes}-{self.current_term}")
                
    def step_down(self, term):
        print(f"Stepping down...")
        
        # Revert to follower state
        self.current_term = term
        self.state = 'FOLLOWER'
        self.voted_for = -1
        
        # Once server becomes follower, set election timeout again
        self.set_election_timeout()
    
    def leader_send_append_entries(self):
        print(f"Sending append entries from leader...")
        
        while True:
            # Check everytime if it is leader before sending append queries
            with self.raft_lock:
                if self.state == 'LEADER':
                    self.append_entries()
                    
                    # Commit entry after it has been replicated
                    last_index, _ = self.commit_log.get_last_index_term()
                    self.commit_index = last_index
                    
    def append_entries(self):
        res = Queue()
            
        for j in range(len(self.partitions[self.cluster_index])):
            if j != self.server_index:
                # Send append entries requests in parallel
                utils.run_thread(fn=self.send_append_entries_request, args=(j,res,))
        
        if len(self.partitions[self.cluster_index]) > 1:        
            cnts = 0
        
            while True:
                # Wait for servers to respond
                res.get(block=True)
                cnts += 1
                # Once we get reply from majority of servers, then return
                # and don't wait for remaining servers
                # Exclude self
                if cnts > (len(self.partitions[self.cluster_index])/2.0)-1:
                    return
        else:
            return
            
    
    def send_append_entries_request(self, server, res=None):
        print(f"Sending append entries to {server}...")
        
        # Fetch previous index and previous term for log matching
        prev_idx = self.next_indices[server]-1
        
        # Get all logs from prev_idx onwards, because all logs after prev_idx will be
        # used to replicate to server
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
                
                # If timeout happens resp returns None, so it won't go inside this condition
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
        
        # If term < self.current_term then the append request came from an old leader
        # and we should not take action in that case.
        with self.raft_lock:
            if term > self.current_term:
                # Most likely term == self.current_term, if this server participated 
                # in voting rounds. If server was down during voting rounds and previous append requests, then term > self.current_term
                # and we should update its term 
                self.step_down()
            
            if term == self.current_term:
                # Request came from current leader
                self.leader_id = server
                
                # Check if the term corresponding to the prev_idx matches with that of the leader
                self_logs = self.commit_log.read_logs_start_end(prev_idx, prev_idx) if prev_idx != -1 else []
                
                # Even with retries, this is idempotent
                success = prev_idx == -1 or (len(self_logs) > 0 and self_logs[0][0] == prev_term)
                
                if success:
                    # On retry, we will overwrite the same logs
                    last_index, last_term = self.commit_log.get_last_index_term()
                    
                    if len(logs) > 0 and last_term == logs[-1][0] and last_index == self.commit_index:
                        # Check if last term in self log matches leader log and last index in self log matches commit index
                        # Then this is a retry and will avoid overwriting the logs
                        index = self.commit_index
                    else:
                        index = self.store_entries(prev_idx, logs)
                    
                flag = 1 if success else 0
                
                # Update election timeout because we received heartbeat from leader
                self.set_election_timeout()
            
            return f"APPEND-REP {self.server_index} {self.current_term} {flag} {index}"
            
    def process_append_reply(self, server, term, success, index):
        print(f"Processing append reply from {server} {term}...")
        
        # It cannot be possible that term < self.current_term because at the time of append request, 
        # all servers will be updated to current term
        
        if term > self.current_term:
            # This could be an old leader which become active and sent
            # append requests but on receiving higher term, it will revert
            # back to follower state.
            self.step_down(term)
            
        if self.state == 'LEADER' and term == self.current_term:
            if success:
                # If server log repaired successfully from leader then increment next index
                self.next_indices[server] = index+1
            else:
                # If server log could not be repaired successfully, then retry with 1 index less lower than current next index
                # Process repeats until we find a matching index and term on server
                self.next_indices[server] = max(0, self.next_indices[server]-1)
                self.send_append_entries_request(server)  
                
    def store_entries(self, prev_idx, leader_logs):
        # Update/Repair server logs from leader logs, replacing non-matching entries and adding non-existent entries
        # Repair starts from prev_idx+1 where prev_idx is the index till where
        # both leader and server logs match.
        commands = [f"{leader_logs[i][1]}" for i in range(len(leader_logs))]
        last_index, _ = self.commit_log.log_replace(self.current_term, commands, prev_idx+1)
        self.commit_index = last_index
        
        # Update state machine
        for command in commands:
            self.update_state_machine(command)
        
        return last_index
    
    def update_state_machine(self, command):
        # Update state machine i.e. in memory hash map in this case
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