from datetime import datetime
from threading import Lock

class CommitLog:
    def __init__(self, file='commit-log.txt'):
        self.file = file
        self.lock = Lock()
        
    def log(self, command, sep=" "):
        with self.lock:
            with open(self.file, 'a') as f:
                now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                message = now + "," + command
                f.write(f"{message}\n")
    
    def read_log(self):
        with self.lock:
            output = []
            with open(self.file, 'r') as f:
                for line in f:
                    _, command = line.strip().split(",")
                    output += [command]
            
            return output