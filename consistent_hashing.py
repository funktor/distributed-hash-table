from sortedcontainers import SortedList
import mmh3, time
from threading import Lock

class ConsistentHashing:
    def __init__(self, multiplier=10):
        self.node_hashes = SortedList(key=lambda x: x[0])
        self.key_hashes = SortedList(key=lambda x: x[0])
        self.node_multiplier = multiplier
        self.lock = Lock()
    
    def add_node_hash(self, node_id):
        with self.lock:
            for i in range(self.node_multiplier):
                h = mmh3.hash(node_id + str(i), signed=False)
                self.node_hashes.add((h, node_id))
    
    def add_key_hash(self, key):
        with self.lock:
            h = mmh3.hash(key, signed=False)
            self.key_hashes.add((h, key))
        
    def get_next_node(self, key):
        with self.lock:
            h = mmh3.hash(key, signed=False)
            if len(self.node_hashes) > 0:
                index = self.node_hashes.bisect_left((h, ''))
                return self.node_hashes[index % len(self.node_hashes)][1]
            return None
    
    def node_exists(self, node_id):
        with self.lock:
            for i in range(self.node_multiplier):
                h = mmh3.hash(node_id + str(i), signed=False)
                try:
                    j = self.node_hashes.index((h, node_id))
                except:
                    return False
        
            return True
    
    def key_exists(self, key):
        with self.lock:
            h = mmh3.hash(key, signed=False)
            try:
                j = self.key_hashes.index((h, key))
            except:
                return False
            
            return True
        