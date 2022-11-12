from datetime import datetime
from threading import Lock
import os, tqdm

class CommitLog:
    def __init__(self, file='commit-log.txt'):
        self.file = file
        self.lock = Lock()
        
    def truncate(self):
        with self.lock:
            with open(self.file, 'w') as f:
                f.truncate()
        
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
    
    def write_log_from_sock(self, sock):
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