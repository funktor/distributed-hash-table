from datetime import datetime
from threading import Lock
import os, tqdm

class CommitLog:
    def __init__(self, file='commit-log.txt'):
        self.file = file
        self.lock = Lock()
        self.last_term = 0
        self.last_index = -1
        
    def truncate(self):
        # Truncate file
        with self.lock:
            with open(self.file, 'w') as f:
                f.truncate()
        
        self.last_term = 0
        self.last_index = -1
        
    def log(self, term, command):
        # Append the term and command to file along with timestamp
        with self.lock:
            with open(self.file, 'a') as f:
                now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                message = f"{now},{term},{command}"
                f.write(f"{message}\n")
                self.last_term = term
                self.last_index += 1
                
    def log_replace(self, term, commands, start):
        # Replace or Append multiple commands starting at 'start' index line number in file
        index = 0
        i = 0
        with self.lock:
            with open(self.file, 'r+') as f:
                if len(commands) > 0:
                    while i < len(commands):
                        if index >= start:
                            command = commands[i]
                            i += 1
                            now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                            message = f"{now},{term},{command}"
                            f.write(f"{message}\n")
                            
                            if index > self.last_index:
                                self.last_term = term
                                self.last_index = index
                        
                        index += 1
                    
                    # Truncate all lines coming after last command.
                    f.truncate()
        
        return index-1
    
    def read_log(self):
        # Return in memory array of term and command
        with self.lock:
            output = []
            with open(self.file, 'r') as f:
                for line in f:
                    _, term, command = line.strip().split(",")
                    output += [(term, command)]
            
            return output
        
    def read_logs_start_end(self, start, end=None):
        # Return in memory array of term and command between start and end indices
        with self.lock:
            output = []
            index = 0
            with open(self.file, 'r') as f:
                for line in f:
                    if index >= start:
                        _, term, command = line.strip().split(",")
                        output += [(term, command)]
                    
                    index += 1
                    if end and index > end:
                        break
            
            return output
    
    def write_log_from_sock(self, sock):
        # Read from socket and write to log
        with self.lock:
            file_name = self.file
            file_size = os.path.getsize(file_name)
            BUFFER_SIZE = 4096
            
            sock.send("commitlog".encode())
            
            progress = tqdm.tqdm(range(file_size), f"Receiving {file_name}", unit="B", unit_scale=True, unit_divisor=1024)
            
            with open(self.file, 'ab') as f:
                while True:
                    bytes_read = sock.recv(BUFFER_SIZE)
                    if not bytes_read:
                        break
                    f.write(bytes_read)
                    progress.update(len(bytes_read))
                    
    def send_log_to_sock(self, sock):
        # Send log through socket
        with self.lock:
            file_name = self.file
            file_size = os.path.getsize(file_name)
            BUFFER_SIZE = 4096
            
            progress = tqdm.tqdm(range(file_size), f"Sending {file_name}", unit="B", unit_scale=True, unit_divisor=1024)
            with open(file_name, "rb") as f:
                while True:
                    bytes_read = f.read(BUFFER_SIZE)
                    if not bytes_read:
                        break
                    sock.sendall(bytes_read)
                    progress.update(len(bytes_read))