import sys, random, os, re
from time import time
from commit_log import CommitLog
from service import Service
import utils, constants
import time

class Route(Service):
    def __init__(self, service_id, replica_id, port=5001, default_timeout=86400):
        super().__init__(service_id, replica_id, port, default_timeout)
        self.running_instances = []
        
    def healthcheck(self):
        while True:
            try:
                with self.lock:
                    response = self.send_and_receive_message_addr(constants.HEALTHCHECK_SERVICE_HOST, 
                                                            constants.HEALTHCHECK_SERVICE_PORT, 
                                                            msg="hello",
                                                            num_bytes=1024*1024, timeout=60)
                
                    self.running_instances = eval(response)
                    print(f"Running instances are {self.running_instances}")
                    
                    for _, _, ip, port in self.running_instances:
                        added = False
                        
                        for u_ip, u_port, _, _ in self.downstreams:
                            if (ip, port) == (u_ip, u_port):
                                added = True
                                break
                        
                        if added is False:
                            self.create_downstream_connections(ip, port)
                    
            except:
                print("Error doing healthcheck")
                
            time.sleep(5)
        
    def run_healthcheck(self):
        self.create_downstream_connections(constants.HEALTHCHECK_SERVICE_HOST, 
                                           constants.HEALTHCHECK_SERVICE_PORT)
        
        utils.start_thread(self.healthcheck)
    
    def get_running_instances(self):
        with self.lock:
            return self.running_instances
    
    def setup(self):
        self.run_healthcheck()
            
    def write(self, id, msg):
        running_instances = self.get_running_instances()
        valid_instances = [x for x in running_instances if x[1] == id]
        
        rnd = random.randint(0, len(valid_instances)-1)
        _, _, ip, port = valid_instances[rnd]
        
        return self.send_and_receive_message_addr(ip, port, msg)
    
    def read(self, id, msg):
        running_instances = self.get_running_instances()
        valid_instances = [x for x in running_instances if x[1] == id]
        
        rnd = random.randint(0, len(valid_instances)-1)
        _, _, ip, port = valid_instances[rnd]
        
        return self.send_and_receive_message_addr(ip, port, msg)
    
    def set_hash_partition(self, msg, key):
        running_instances = self.get_running_instances()
        unique_ids = list(set([x[1] for x in running_instances]))
        
        index = hash(key) % len(unique_ids)
        id = unique_ids[index]
        return self.write(id, msg)
    
    def get_hash_partition(self, msg, key):
        running_instances = self.get_running_instances()
        unique_ids = list(set([x[1] for x in running_instances]))
        
        index = hash(key) % len(unique_ids)
        id = unique_ids[index]
        return self.read(id, msg)
    
    def handle_commands(self, msg):
        set_ht = re.match('^set ([a-zA-Z0-9]+) ([a-zA-Z0-9]+)$', msg)
        get_ht = re.match('^get ([a-zA-Z0-9]+)$', msg)
            
        if set_ht:
            key, _ = set_ht.groups()
            msg = f"{msg} {time.time()}"
            output = self.set_hash_partition(msg, key)
        
        elif get_ht:
            key = get_ht.groups()
            key = key[0]
            output = self.get_hash_partition(msg, key)
        
        else:
            output = "Error: Invalid command"
        
        return output
    
    def process_request(self, conn):
        while True:
            try:
                msg = self.receive_message_conn(conn)
                print(f"{msg} received")
                
                output = self.handle_commands(msg)
                
                self.send_message_conn(conn, output)
            except:
                print("Could not process client request")

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print ("Correct usage: script, service_id, replica_id, IP address, port number")
        exit()

    service_id = str(sys.argv[1])
    replica_id = str(sys.argv[2])
    ip_address = str(sys.argv[3])
    port = int(sys.argv[4])
    
    route_obj = Route(service_id=service_id, replica_id=replica_id, port=port)
    route_obj.register(service_id, replica_id, ip_address, port)
    route_obj.setup()
    route_obj.listen_to_clients()
