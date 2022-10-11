import sys, re, time, utils
from service import Service
from consistent_hashing import ConsistentHashing

class ConsistentHashingService(Service):
    def __init__(self, service_id, cluster_id, ip, port, default_timeout=86400):
        super().__init__(service_id, cluster_id, ip, port, default_timeout)
        self.chash = ConsistentHashing()
        self.register(service_id, cluster_id, ip, port)
        self.use_replication()
        
    def update(self, msg):
        self.set_log_command(msg)
        utils.start_thread(self.broadcast_write, args=(msg,))
    
    def handle_commands(self, msg):        
        get_node_id = re.match('^get-node ([a-zA-Z0-9]+)$', msg)
        put_node_id = re.match('^put-node ([_\.a-zA-Z0-9]+)$', msg)
        put_keys_id = re.match('^put-keys ([a-zA-Z0-9]+)$', msg)
        log = re.match('^commitlog ([0-9]+) ([0-9]+)$', msg)
        health = re.match('^hello$', msg)
        
        if get_node_id:
            key = get_node_id.groups()
            key = key[0]
            output = self.chash.get_next_node(key)
            
        elif put_node_id:
            node_id = put_node_id.groups()
            node_id = node_id[0]
            
            if self.chash.node_exists(node_id):
                output = "Node Exists"
            else:
                self.commit_log.log(msg)
                self.chash.add_node_hash(node_id)
                self.update(msg)
                output = "Node Added"
        
        elif put_keys_id:
            key = put_keys_id.groups()
            key = key[0]
            
            if self.chash.key_exists(key):
                output = "Key Exists"
            else:
                self.commit_log.log(msg)
                self.chash.add_key_hash(key)
                self.update(msg)
                output = "Key Added"
        
        elif log:
            start, end = log.groups()
            start, end = int(start), int(end)
            if start > end:
                output = "Error: start is greater than end"
            else:
                commands = self.get_log_commands(start, end)
                output = str(commands)
        
        elif health:
            output = 'world'
        
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
    
    con_hashing = ConsistentHashingService(service_id=service_id, 
                                           cluster_id=cluster_id, 
                                           ip=ip_address, 
                                           port=port)
    con_hashing.listen_to_clients()