from sortedcontainers import SortedList
import mmh3, time
from threading import Lock

class ConsistentHashing:
    def __init__(self, multiplier=10):
        self.node_hashes = SortedList(key=lambda x: x[0])
        self.node_multiplier = multiplier
        self.lock = Lock()
    
    def add_node_hash(self, node_id):
        existing = self.node_exists(node_id)
        
        with self.lock:
            if existing is False:
                for i in range(self.node_multiplier):
                    h = mmh3.hash(node_id + str(i), signed=False)
                    self.node_hashes.add((h, node_id))
                return 1
            return -1
        
    def get_next_node(self, key):
        with self.lock:
            h = mmh3.hash(key, signed=False)
            if len(self.node_hashes) > 0:
                index = self.node_hashes.bisect_left((h, ''))
                return self.node_hashes[index % len(self.node_hashes)][1]
            return None
    
    def get_next_nodes_from_node(self, node_id):
        nodes = set()
        
        with self.lock:
            for i in range(self.node_multiplier):
                h = mmh3.hash(node_id + str(i), signed=False)
                index = self.node_hashes.bisect_left((h, node_id))
                next_node = self.node_hashes[(index+1) % len(self.node_hashes)][1]
                
                if next_node != node_id:
                    nodes.add(next_node)
                    
        return nodes
    
    def node_exists(self, node_id):
        with self.lock:
            for i in range(self.node_multiplier):
                h = mmh3.hash(node_id + str(i), signed=False)
                try:
                    j = self.node_hashes.index((h, node_id))
                except:
                    return False
        
            return True
        