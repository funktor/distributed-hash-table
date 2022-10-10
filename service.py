import socket, select, os, time
from threading import Thread, Lock
import constants, utils, mmh3
from commit_log import CommitLog

def get_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    return sock

class Service:
    def __init__(self, service_id, cluster_id, ip, port, default_timeout=86400):
        self.service_id = service_id
        self.cluster_id = cluster_id
        self.upstream_connector = None
        self.upstreams = []
        self.downstreams = {}
        self.ip = ip
        self.port = port
        self.default_timeout = default_timeout
        self.lock = Lock()
        self.socket_locks = {}
        self.create_upstream_connector(self.port)
        self.commit_log = CommitLog(file=f"commit-log-{service_id}.txt")
        self.replicas = []
        self.commands = []
        self.commands_lock = Lock()
        self.replicas_lock = Lock()
    
    def setup(self):
        pass
        
    def register(self, service_id, cluster_id, ip, port):
        self.create_downstream_connections(constants.REGISTRY_SERVICE_HOST, 
                                           constants.REGISTRY_SERVICE_PORT)
        
        response = \
            self.send_and_receive_message_addr(\
                constants.REGISTRY_SERVICE_HOST, 
                constants.REGISTRY_SERVICE_PORT, 
                f"put {service_id} {cluster_id} {ip} {port}")
        
        print(f"Response {response} received from registry service")
        
    def use_replication(self):
        utils.start_thread(self.read_command_logs)
        utils.start_thread(self.get_and_connect_replicas)
    
    def read_command_logs(self):
        while True:
            with self.commands_lock:
                if os.path.exists(self.commit_log.file):
                    self.commands = self.commit_log.read_log()
                    
    def get_and_connect_replicas(self):
        self.create_downstream_connections(constants.HEALTHCHECK_SERVICE_HOST, 
                                           constants.HEALTHCHECK_SERVICE_PORT)
        
        while True:
            with self.replicas_lock:
                response = \
                    self.send_and_receive_message_addr(\
                        constants.HEALTHCHECK_SERVICE_HOST, 
                        constants.HEALTHCHECK_SERVICE_PORT, 
                        msg="hello", 
                        num_bytes=1024*1024, 
                        timeout=60)
                
                print(f"Response {response} received from healthcheck service")
                
                nodes = eval(response)
                self.replicas = [x for x in nodes if x[1] == self.cluster_id]
                
                for sid, cid, ip, port in self.replicas:
                    if sid != self.service_id:
                        port = int(port)
                        self.create_downstream_connections(ip, port)
                        utils.start_thread(self.sync_commit_log, args=(ip, port,))
                        
                print(f"Active replicas are {self.replicas}")
                
            time.sleep(2)
    
    def sync_commit_log(self, rip, rport, max_commands=5):
        self.create_downstream_connections(rip, rport)
        
        start, end = 0, max_commands-1
        while True:
            response = self.send_and_receive_message_addr(rip, 
                                                          rport, 
                                                          f"commitlog {start} {end}", 
                                                          num_bytes=1024*1024, 
                                                          timeout=240)
            
            print(f"Response {response} received from {rip}:{rport}")
            response = eval(response)
            
            for msg in response:
                self.handle_commands(msg)
            
            k = len(response)
            end = start+k-1
            
            start = end+1
            end = start+max_commands-1
    
    def broadcast_write(self, msg):
        for sid, _, ip, port in self.get_replicas():
             if sid != self.service_id:
                 self.create_downstream_connections(ip, port)
                 self.send_and_receive_message_addr(ip, port, msg)
    
    def get_replicas(self):
        with self.replicas_lock:
            return self.replicas
            
    def get_log_commands(self, start, end):
        with self.commands_lock:
            return self.commands[start:min(end+1, len(self.commands))]
    
    def set_log_command(self, command):
        with self.commands_lock:
            self.commands += [command]
        
    def setup_load_balancing(self):
        self.create_downstream_connections(constants.CONSISTENT_HASH_SERVICE_HOST, 
                                           constants.CONSISTENT_HASH_SERVICE_PORT)
        
        shash = mmh3.hash(self.cluster_id + str(0), signed=False)
        
        if int(self.service_id) == int(shash):
            try:
                response = \
                    self.send_and_receive_message_addr(\
                        constants.CONSISTENT_HASH_SERVICE_HOST, 
                        constants.CONSISTENT_HASH_SERVICE_PORT, 
                        f"put-node {self.service_id + '_' + self.cluster_id + '_' + self.ip + '_' + str(self.port)}")
                
                print(f"Response {response} received from consistent hashing service")
            except Exception as e:
                print("The error raised is: ", e)
                
        
    def create_upstream_connector(self, port):
        sock = get_socket()
        sock.bind((constants.DEFAULT_IP_ADDRESS, int(port)))
        sock.listen(constants.MAX_CONNECTIONS)
        
        self.upstream_connector = sock
    
    def create_downstream_connections(self, ip_address, port):
        if (ip_address, port) not in self.downstreams:
            sock = get_socket()
            sock.setblocking(False)
            sock.settimeout(self.default_timeout)
            sock.connect((str(ip_address), int(port)))
            
            self.downstreams[(ip_address, port)] = sock
            self.socket_locks[(ip_address, port)] = Lock()
        
    def listen_to_clients(self):
        while True:
            try:
                client_socket, client_address = self.upstream_connector.accept()
                print(f"Connected to new client at address {client_address}")
                self.upstreams += [client_socket]
                
                utils.start_thread(fn=self.process_request, args=(client_socket,))
            except:
                print("Error accepting connection...")
            
    def send_message_conn(self, conn, msg):
        try:
            conn.send(msg.encode())
        except:
            print(f"Could not send message {msg} to {conn}")
    
    def send_message_addr(self, ip_address, port, msg):
        if (ip_address, port) in self.downstreams:
            conn = self.downstreams[(ip_address, port)]
            self.send_message_conn(conn, msg)
            return
    
        raise Exception("Invalid ip and port provided")
    
    def receive_message_conn(self, conn, num_bytes=2048, timeout=30):
        try:
            ready = select.select([conn], [], [], timeout)
            if ready[0]:
                return conn.recv(num_bytes).decode()
            else:
                return None
        except:
            print(f"Could not receive message from {conn}")
            return None
    
    def receive_message_addr(self, ip_address, port, num_bytes=2048, timeout=30):
        if (ip_address, port) in self.downstreams:
            conn = self.downstreams[(ip_address, port)]
            return self.receive_message_conn(conn, num_bytes, timeout)
        
        raise Exception("Invalid ip and port provided")
    
    def handle_commands(self, msg):
        pass
    
    def process_request(self, conn):
        while True:
            try:
                msg = self.receive_message_conn(conn, timeout=86400)
                print(f"{msg} received")
                output = self.handle_commands(msg)
                
                self.send_message_conn(conn, output)
                
            except Exception as e:
                print("Error processing message from client")
                print(e)
    
    def send_and_receive_message_conn(self, conn, msg, num_bytes=2048, timeout=30):
        self.send_message_conn(conn, msg)
        resp = self.receive_message_conn(conn, num_bytes, timeout)
        return resp
    
    def send_and_receive_message_addr(self, ip, port, msg, num_bytes=2048, timeout=30):
        with self.socket_locks[(ip, port)]:
            self.send_message_addr(ip, port, msg)
            resp = self.receive_message_addr(ip, port, num_bytes, timeout)
            return resp

    