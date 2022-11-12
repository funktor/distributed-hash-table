from queue import Queue
import select
import socket
from threading import Thread
import time


def run_thread(fn, args):
    my_thread = Thread(target=fn, args=args)
    my_thread.daemon = True
    my_thread.start()
    return my_thread
            
def send_and_recv(msg, servers, socket_locks, i, res=None, timeout=-1):
    resp = None
    # Could not connect possible reasons:
    # 1. Server is not ready
    # 2. Server is busy and not responding
    # 3. Server crashed and not responding
    
    start = time.time()
    
    while True:
        try:
            if servers[i][2] is None:
                # Server has not been connected yet. If server is not ready
                # throws error.
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                ip, port, _ = servers[i]
                sock.connect((str(ip), int(port)))
                servers[i][2] = sock
                
            with socket_locks[i]:
                conn = servers[i][2]   
                # If server crashed then this will throw error. 
                conn.send(msg.encode())
                if timeout > 0:
                    ready = select.select([conn], [], [], timeout)
                    if ready[0]:
                        resp = conn.recv(2048).decode()
                else:
                    resp = conn.recv(2048).decode()
                    
                break
                    
        except Exception as e:
            print(e)
            # The server crashed but it is still not marked in current node
            if servers[i][2] is not None:
                sock = servers[i][2]
                sock.close()
                servers[i][2] = None
            
            # wait for 0.5 seconds before requesting again
            time.sleep(0.5)
            
    if res is not None:
        res.put(resp)
        
    return resp
                
def broadcast_write(msg, cluster, lock, socket_locks):
    # Add the outputs from sending to replicas in a multithreaded queue
    res = Queue()
    
    with lock:
        n = len(cluster)
        
    if n == 1:
        # No replica found
        return True
    
    for i in range(n):
        if cluster[i] is not None:
            # Send message to replicas in parallel threads
            run_thread(send_and_recv, args=(msg, cluster, socket_locks, i, res))
            
    cnts = 0
    
    while True:
        try:
            # Wait for all replicas to respond
            out = res.get(block=True)
            if out and out == 'ok':
                cnts += 1
                # Exclude the leader because leader is already updated
                if cnts == n-1:
                    return True
            else:
                return False
        except Exception as e:
            print(e)
            return False

def broadcast_join(msg, conns, lock, socket_locks, exclude=None):
    # Add the outputs from sending to replicas in a multithreaded queue
    res = Queue()
    
    with lock:
        n = len(conns)
    
    for i in range(n):
        # Send message to all leaders in parallel threads (except self)
        if exclude is None or exclude != i:
            run_thread(send_and_recv, args=(msg, conns[i], socket_locks[i], 0, res))

    cnts = 0
    
    while True:
        try:
            out = res.get(block=True)
            if out and out == 'ok':
                cnts += 1
                # All leaders received the join request
                if (exclude is None and cnts == n) or (exclude is not None and cnts == n-1):
                    return True
            else:
                return False
        except Exception as e:
            print(e)
            return False