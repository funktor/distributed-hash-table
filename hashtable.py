class HashTable:
    def __init__(self):
        self.map = {}
    
    def set(self, key, value, timestamp):
        self.map[key] = (value, timestamp)
    
    def get_value(self, key):
        if key in self.map:
            return self.map[key][0]
        return None

    def get_timestamp(self, key):
        if key in self.map:
            return self.map[key][1]
        return None
