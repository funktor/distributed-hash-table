from threading import Lock 
from copy import deepcopy

class HashTable:
    def __init__(self):
        self.map = {}
        self.lock = Lock()
    
    def get_copy(self):
        with self.lock:
            return deepcopy(self.map)
    
    def set_copy(self, cpy):
        with self.lock:
            self.map = cpy
    
    def set(self, key, value, req_id):
        with self.lock:
            if key not in self.map or self.map[key][1] < req_id:
                self.map[key] = (value, req_id)
                return 1
            return -1
    
    def get_value(self, key):
        with self.lock:
            if key in self.map:
                return self.map[key]
            return None

    def get_req_id(self, key):
        with self.lock:
            if key in self.map:
                return self.map[key][1]
            return None
    
    def delete(self, key, req_id):
        with self.lock:
            if key in self.map and self.map[key][1] <= req_id:
                self.map.pop(key)
                return 1
            return -1
