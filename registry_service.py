import sys, re
from registry import Registry
from service import Service

class RegistryService(Service):
    def __init__(self, service_id, cluster_id, ip, port, default_timeout=86400):
        super().__init__(service_id, cluster_id, ip, port, default_timeout)
        self.register(service_id, cluster_id, ip, port)
        
    def register(self, sid, cid, ip, port):
        self.registry_obj = Registry()
        self.registry_obj.register(sid, cid, ip, port)
    
    def handle_commands(self, msg):
        get = re.match('^get$', msg)
        put = re.match('^put ([a-zA-Z0-9]+) ([a-zA-Z0-9]+) ([0-9\.]+) ([0-9]+)$', msg)
        replicas = re.match('^get-replicas ([a-zA-Z0-9]+)$', msg)
        health = re.match('^hello$', msg)
        
        if health:
            output = 'world'
        
        elif get:
            output = str(self.registry_obj.get_entries())
            
        elif put:
            sid, cid, ip, port = put.groups()
            self.register(sid, cid, ip, port)
            output = "Logged"
        
        elif replicas:
            rep_id = replicas.groups()
            rep_id = rep_id[0]
            
            reps = self.registry_obj.get_entries()
            reps = [x for x in reps if x[1] == rep_id]
            output = str(reps)
        
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
    
    registry = RegistryService(service_id=service_id, 
                               cluster_id=cluster_id, 
                               ip=ip_address, 
                               port=port)
    
    registry.listen_to_clients()