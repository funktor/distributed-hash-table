from threading import Lock 

class HashTable:
    def __init__(self):
        self.map = {}
        self.lock = Lock()
    
    def set(self, key, value, req_id):
        with self.lock:
            self.map[key] = (value, req_id)
    
    def get_value(self, key):
        with self.lock:
            if key in self.map:
                return self.map[key][0]
            return None

    def get_req_id(self, key):
        with self.lock:
            if key in self.map:
                return self.map[key][1]
            return None
