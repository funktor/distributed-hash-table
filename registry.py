from threading import Lock

class Registry:
    def __init__(self):
        self.file = 'registry.txt'
        self.lock = Lock()
    
    def register(self, service_id, replica_id, ip_address, port):
        with self.lock:
            with open(self.file, 'a') as f:
                f.write(f"{service_id} {replica_id} {ip_address} {port}\n")
    
    def get_entries(self):
        with self.lock:
            output = []
            with open(self.file, 'r') as f:
                for line in f:
                    sid, rid, ip, port = line.strip().split(" ")
                    output.append((sid, rid, ip, port))
            
            return output
        