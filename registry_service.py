import sys, re
from registry import Registry
from service import Service

class RegistryService(Service):
    def register(self, sid, rid, ip, port):
        self.registry_obj = Registry()
        self.registry_obj.register(sid, rid, ip, port)
    
    def handle_commands(self, msg):
        get = re.match('^get$', msg)
        put = re.match('^put ([a-zA-Z0-9]+) ([a-zA-Z0-9]+) ([0-9\.]+) ([0-9]+)$', msg)
        replicas = re.match('^get-replicas ([a-zA-Z0-9]+)$', msg)
        
        if get:
            output = str(self.registry_obj.get_entries())
            
        elif put:
            sid, rid, ip, port = put.groups()
            self.register(sid, rid, ip, port)
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
    
    def process_request(self, conn):
        while True:
            try:
                msg = self.receive_message_conn(conn, timeout=86400)
                print(f"{msg} received")
                output = self.handle_commands(msg)
                
                self.send_message_conn(conn, output)
                
            except:
                print("Error processing message from client")

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print ("Correct usage: script, service_id, replica_id, IP address, port number")
        exit()

    service_id = str(sys.argv[1])
    replica_id = str(sys.argv[2])
    ip_address = str(sys.argv[3])
    port = int(sys.argv[4])
    
    registry = RegistryService(service_id=service_id, replica_id=replica_id, port=port)
    registry.register(service_id, replica_id, ip_address, port)
    registry.listen_to_clients()