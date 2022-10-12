import sys, os, re
from hashtable import HashTable
from service import Service
import utils, time, constants, mmh3

class HashTableService(Service):
    def __init__(self, service_id, cluster_id, ip, port, default_timeout=86400):
        super().__init__(service_id, cluster_id, ip, port, default_timeout)
        self.ht = HashTable()
        self.register(service_id, cluster_id, ip, port)
        self.use_replication()
        self.setup_load_balancing()
        
    def route(self, key):
        self.create_downstream_connections(constants.CONSISTENT_HASH_SERVICE_HOST, 
                                           constants.CONSISTENT_HASH_SERVICE_PORT)
        
        response = \
            self.send_and_receive_message_addr(\
                constants.CONSISTENT_HASH_SERVICE_HOST, 
                constants.CONSISTENT_HASH_SERVICE_PORT, 
                f"get-node {key}")
        
        print(response)
        node_service_id, node_cluster_id, node_ip, node_port = response.split('_')
        node_port = int(node_port)
        
        return node_service_id, node_cluster_id, node_ip, node_port
    
    def forward(self, node_ip, node_port, msg):
        self.create_downstream_connections(node_ip, node_port)
        self.send_and_receive_message_addr(node_ip, node_port, msg)
        
    def handle_commands(self, msg):
        set_ht = re.match('^set ([a-zA-Z0-9]+) ([a-zA-Z0-9]+) ([0-9]+)$', msg)
        get_ht = re.match('^get ([a-zA-Z0-9]+) ([0-9]+)$', msg)
        log = re.match('^commitlog ([0-9]+) ([0-9]+)$', msg)
        health = re.match('^hello$', msg)
        
        if health:
            output = 'world'
            
        elif set_ht:
            key, value, req_id = set_ht.groups()
            req_id = int(req_id)
            
            _, node_cluster_id, node_ip, node_port = self.route(key)
            print(node_ip, node_port)
            print(self.ip, self.port)
            
            if node_cluster_id != self.cluster_id:
                utils.start_thread(self.forward, args=(node_ip, node_port, msg))
                output = 'Forwarded'
            else:
                res = self.ht.set(key=key, value=value, req_id=req_id)
                
                if res == 1:
                    self.commit_log.log(msg)
                    self.set_log_command(msg)
                    utils.start_thread(self.broadcast_write, args=(msg,))
                    utils.start_thread(self.send_and_receive_message_addr, 
                                        args=(constants.CONSISTENT_HASH_SERVICE_HOST, 
                                                constants.CONSISTENT_HASH_SERVICE_PORT, 
                                                f"put-keys {key}", 2048, 30))
                    
                output = "Inserted"
        
        elif get_ht:
            key, req_id = get_ht.groups()
            _, node_cluster_id, node_ip, node_port = self.route(key)
            
            if node_cluster_id != self.cluster_id:
                self.create_downstream_connections(node_ip, node_port)
                output = self.send_and_receive_message_addr(node_ip, node_port, msg)
            else: 
                output = self.ht.get_value(key=key)
                
            if output is None:
                output = 'Error: Non existent key'
        
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

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print ("Correct usage: script, service_id, cluster_id, IP address, port number")
        exit()

    service_id = str(sys.argv[1])
    cluster_id = str(sys.argv[2])
    ip_address = str(sys.argv[3])
    port = int(sys.argv[4])
    
    dht = HashTableService(service_id=service_id, 
                           cluster_id=cluster_id, 
                           ip=ip_address, 
                           port=port)
    dht.listen_to_clients()