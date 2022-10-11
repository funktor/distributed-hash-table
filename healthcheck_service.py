import sys, time, os
from service import Service
import constants, utils

class HealthCheckService(Service):
    def __init__(self, service_id, cluster_id, ip, port=5001, default_timeout=86400, max_wait_time=5, interval=10):
        super().__init__(service_id, cluster_id, ip, port, default_timeout)
        self.interval = interval
        self.max_wait_time = max_wait_time
        self.active_connections = set()
        self.register(service_id, cluster_id, ip_address, port)
        utils.start_thread(self.start_polling)
        
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

    def start_new_service(self, service_id, cluster_id, ip, port):
        self.remove_from_active_connections((service_id, cluster_id, ip, port))
        os.system(f"start \"HashTable Service {service_id}-{cluster_id}-{ip}-{port}\" python \"hashtable_service.py\" \"{service_id}\" \"{cluster_id}\" \"{ip}\" \"{port}\"")
        self.add_to_active_connections((service_id, cluster_id, ip, port))
        
    def poll_server(self, service_id, cluster_id, ip, port):
        while True:
            try:
                res = self.send_and_receive_message_addr(ip, 
                                                        port, 
                                                        "hello", 
                                                        timeout=self.max_wait_time)
                
                if res:
                    print(f"Response {res} received from server {ip}:{port}")
                    self.add_to_active_connections((service_id, cluster_id, ip, port))
                else:
                    print(f"No response received from server {ip}:{port}")
                    utils.start_thread(self.start_new_service, args=(service_id, cluster_id, ip, port))
                        
            except Exception as e:
                print(e)
                
            time.sleep(self.interval)
        
    def start_polling(self):
        while True:
            try:
                response = \
                    self.send_and_receive_message_addr(\
                        constants.REGISTRY_SERVICE_HOST, 
                        constants.REGISTRY_SERVICE_PORT, 
                        "get", 
                        num_bytes=1024*1024, 
                        timeout=60)
                
                print(f"Response {response} received from registry service")
                
                running_instances = eval(response)
            
                for sid, cid, ip, port in running_instances:
                    if sid != self.service_id and (ip, port) not in self.downstreams:
                        self.create_downstream_connections(ip, port)
                        utils.start_thread(self.poll_server, 
                                           args=(sid, cid, ip, port, ))
            
            except Exception as e:
                print(e)
                
            time.sleep(self.interval)
            
    def handle_commands(self, msg):
        active_connections = self.get_active_connections()
        return str(list(active_connections))

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print ("Correct usage: script, service_id, cluster_id, IP address, port number")
        exit()

    service_id = str(sys.argv[1])
    cluster_id = str(sys.argv[2])
    ip_address = str(sys.argv[3])
    port = int(sys.argv[4])
    
    health = HealthCheckService(service_id=service_id, 
                                cluster_id=cluster_id, 
                                ip=ip_address,
                                port=port, 
                                max_wait_time=20, 
                                interval=5)
    
    health.listen_to_clients()
        