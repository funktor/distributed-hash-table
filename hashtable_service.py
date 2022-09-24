import sys, os, re
from hashtable import HashTable
from service import Service
from commit_log import CommitLog
import utils, time, constants

class HashTableService(Service):
    def setup(self):
        self.ht = HashTable()
        self.commit_log = CommitLog(file=f"commit-log-{self.service_id}.txt")
        self.commands = []
        self.replicas = []
        utils.start_thread(self.read_command_logs)
        utils.start_thread(self.get_active_replicas)
                 
    def broadcast_write(self, msg):
        for sid, rid, ip, port in self.get_replicas():
             if sid != self.service_id:
                 utils.start_thread(lambda x, y, z: self.send_message_addr(x, y, z), 
                                    args=(ip, port, msg))
        
    def get_active_replicas(self):
        while True:
            with self.lock:
                response = self.send_and_receive_message_addr(constants.REGISTRY_SERVICE_HOST, 
                                                            constants.REGISTRY_SERVICE_PORT, 
                                                            f"get-replicas {self.replica_id}")
                
                self.replicas = eval(response)
                print(f"Response {response} received from registry service")
                
                for _, _, ip, port in self.replicas:
                    added = False
                    
                    for u_ip, u_port, _ in self.downstreams:
                        if (ip, port) == (u_ip, u_port):
                            added = True
                            break
                    
                    if added is False:
                        self.create_downstream_connections(ip, port)
                        utils.start_thread(self.sync_commit_log, args=(ip, port,))
                
                print(f"Active replicas are {self.replicas}")
            
            time.sleep(10)
    
    def get_replicas(self):
        with self.lock:
            return self.replicas
        
    def sync_commit_log(self, rip, rport):
        start, end = 0, 4
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
            
            start, end = end+1, end+5
        
    def read_command_logs(self):
        while True:
            with self.lock:
                if os.path.exists(self.commit_log.file):
                    self.commands = self.commit_log.read_log()
                
            time.sleep(5)
            
    def get_log_commands(self, start, end):
        with self.lock:
            return self.commands[start:end+1]
        
    def handle_commands(self, msg):
        set_ht = re.match('^set ([a-zA-Z0-9]+) ([a-zA-Z0-9]+) ([0-9\.]+)$', msg)
        get_ht = re.match('^get ([a-zA-Z0-9]+)$', msg)
        log = re.match('^commitlog ([0-9]+) ([0-9]+)$', msg)
        health = re.match('^hello$', msg)
        
        if health:
            output = 'world'
            
        elif set_ht:
            key, value, timestamp = set_ht.groups()
            timestamp = float(timestamp)
            old = self.ht.get_timestamp(key=key)
            if old is None or old < timestamp:
                self.ht.set(key=key, value=value, timestamp=timestamp)
                self.commit_log.log(msg)
                self.broadcast_write(msg)
            
            output = "Inserted"
        
        elif get_ht:
            key = get_ht.groups()
            key = key[0]
            output = self.ht.get_value(key=key)
        
        elif log:
            start, end = log.groups()
            start, end = int(start), int(end)
            if start > end:
                output = "Error: start is greater than end"
            else:
                commands = self.get_log_commands(start, end)
                output = str(commands)
        
        else:
            output = "Error: Invalid command"
        
        return output
    
    def process_request(self, conn):
        while True:
            try:
                msg = self.receive_message_conn(conn, timeout=86400)
                print(f"{msg} received")
                
                output = self.handle_commands(msg)
                
                self.send_message_conn(conn, output)
                
            except:
                print("Could not process request from client")

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print ("Correct usage: script, service_id, replica_id, IP address, port number")
        exit()

    service_id = str(sys.argv[1])
    replica_id = str(sys.argv[2])
    ip_address = str(sys.argv[3])
    port = int(sys.argv[4])
    
    dht = HashTableService(service_id=service_id, replica_id=replica_id, port=port)
    dht.register(service_id, replica_id, ip_address, port)
    dht.setup()
    dht.listen_to_clients()