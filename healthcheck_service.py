import sys, time, os
from service import Service
import constants, utils

class HealthCheckService(Service):
    def __init__(self, service_id, replica_id, port=5001, default_timeout=86400, max_wait_time=5, interval=10):
        super().__init__(service_id, replica_id, port, default_timeout)
        self.interval = interval
        self.max_wait_time = max_wait_time
        self.active_connections = set()
        
    def get_active_connections(self):
        with self.lock:
            return self.active_connections
    
    def add_to_active_connections(self, value):
        with self.lock:
            self.active_connections.add(value)
    
    def remove_from_active_connections(self, value):
        with self.lock:
            if value in self.active_connections:
                self.active_connections.remove(value)

    def start_new_service(self, service_id, replica_id, ip, port):
        os.system(f"start \"HashTable Service {service_id}-{replica_id}-{ip}-{port}\" python \"hashtable_service.py\" \"{service_id}\" \"{replica_id}\" \"{ip}\" \"{port}\"")
        k = -1
        for i in range(len(self.downstreams)):
            conn_ip, conn_port, conn = self.downstreams[i]
            if (conn_ip, conn_port) == (ip, port):
                k = i
                break
        
        if k >= 0:
            self.downstreams.pop(k)
            self.create_downstream_connections(ip, port)
        
        self.add_to_active_connections((service_id, replica_id, ip, port))
        
    def poll_server(self, service_id, replica_id, ip, port):
        while True:
            try:
                res = self.send_and_receive_message_addr(ip, 
                                                        port, 
                                                        "hello", 
                                                        timeout=self.max_wait_time)
                
                if res:
                    print(f"Response {res} received from server {ip}:{port}")
                    self.add_to_active_connections((service_id, replica_id, ip, port))
                else:
                    print(f"No response received from server {ip}:{port}")
                    self.remove_from_active_connections((service_id, replica_id, ip, port))
                    utils.start_thread(self.start_new_service, args=(service_id, replica_id, ip, port))
                        
            except:
                print("Could not poll")
                
            time.sleep(self.interval)
        
    def start_polling(self):
        response = self.send_and_receive_message_addr(constants.REGISTRY_SERVICE_HOST, 
                                                      constants.REGISTRY_SERVICE_PORT, 
                                                      "get", 
                                                      num_bytes=1024*1024, 
                                                      timeout=60)
        
        print(f"Response {response} received from registry service")
        
        running_instances = eval(response)
        running_instances = [x for x in running_instances if int(x[1]) > 0]
        
        for sid, rid, ip, port in running_instances:
            self.create_downstream_connections(ip, port)
        
        for sid, rid, ip, port in running_instances:
            utils.start_thread(self.poll_server, args=(sid, rid, ip, port, ))
    
    def process_request(self, conn):
        while True:
            try:
                msg = self.receive_message_conn(conn, timeout=86400)
                print(f"{msg} received")
                
                active_connections = self.get_active_connections()
                message = str(list(active_connections))
                
                self.send_message_conn(conn, message)
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
    
    health = HealthCheckService(service_id=service_id, replica_id=replica_id, 
                                port=port, max_wait_time=20, interval=5)
    
    health.register(service_id, replica_id, ip_address, port)
    health.start_polling()
    health.listen_to_clients()
        