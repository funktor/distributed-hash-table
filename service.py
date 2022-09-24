import socket, select
from threading import Thread, Lock
import constants, utils

def get_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    return sock

class Service:
    def __init__(self, service_id, replica_id, port, default_timeout=86400):
        self.service_id = service_id
        self.replica_id = replica_id
        self.upstream_connector = None
        self.upstreams = []
        self.downstreams = []
        self.port = port
        self.default_timeout = default_timeout
        self.lock = Lock()
        self.create_upstream_connector(self.port)
    
    def setup(self):
        pass
        
    def register(self, service_id, replica_id, ip, port):
        self.create_downstream_connections(constants.REGISTRY_SERVICE_HOST, 
                                           constants.REGISTRY_SERVICE_PORT)
        
        response = self.send_and_receive_message_addr(constants.REGISTRY_SERVICE_HOST, 
                                                      constants.REGISTRY_SERVICE_PORT, 
                                                      f"put {service_id} {replica_id} {ip} {port}")
        
        print(f"Response {response} received from registry service")
        
    def create_upstream_connector(self, port):
        sock = get_socket()
        sock.bind((constants.DEFAULT_IP_ADDRESS, int(port)))
        sock.listen(constants.MAX_CONNECTIONS)
        
        self.upstream_connector = sock
    
    def create_downstream_connections(self, ip_address, port):
        sock = get_socket()
        sock.setblocking(False)
        sock.settimeout(self.default_timeout)
        sock.connect((str(ip_address), int(port)))
        
        self.downstreams += [(ip_address, port, sock)]
        
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
        for conn_ip, conn_port, conn in self.downstreams:
            if (conn_ip, conn_port) == (ip_address, port):
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
        for conn_ip, conn_port, conn in self.downstreams:
            if (conn_ip, conn_port) == (ip_address, port):
                return self.receive_message_conn(conn, num_bytes, timeout)
        
        raise Exception("Invalid ip and port provided")
    
    def process_request(self, conn):
        pass
    
    def send_and_receive_message_conn(self, conn, msg, num_bytes=2048, timeout=30):
        self.send_message_conn(conn, msg)
        resp = self.receive_message_conn(conn, num_bytes, timeout)
        return resp
    
    def send_and_receive_message_addr(self, ip, port, msg, num_bytes=2048, timeout=30):
        self.send_message_addr(ip, port, msg)
        resp = self.receive_message_addr(ip, port, num_bytes, timeout)
        return resp