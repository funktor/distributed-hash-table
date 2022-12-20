from queue import Queue
import select
import socket
from threading import Thread
import time
import traceback

def run_thread(fn, args):
    my_thread = Thread(target=fn, args=args)
    my_thread.daemon = True
    my_thread.start()
    return my_thread

def wait_for_server_startup(ip, port):
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.connect((str(ip), int(port)))
            return sock
                
        except Exception as e:
            traceback.print_exc(limit=1000)

def send_and_recv_no_retry(msg, ip, port, timeout=-1):
    # Could not connect possible reasons:
    # 1. Server is not ready
    # 2. Server is busy and not responding
    # 3. Server crashed and not responding
    
    conn = wait_for_server_startup(ip, port)
    resp = None
    
    try:
        conn.sendall(msg.encode())
        
        if timeout > 0:
            ready = select.select([conn], [], [], timeout)
            if ready[0]:
                resp = conn.recv(2048).decode()
        else:
            resp = conn.recv(2048).decode()
                
    except Exception as e:
        traceback.print_exc(limit=1000)
        # The server crashed but it is still not marked in current node
    
    conn.close()
    return resp
            
def send_and_recv(msg, ip, port, res=None, timeout=-1):
    resp = None
    # Could not connect possible reasons:
    # 1. Server is not ready
    # 2. Server is busy and not responding
    # 3. Server crashed and not responding
    
    while True:
        resp = send_and_recv_no_retry(msg, ip, port, timeout)
        
        if resp:
            break
            
    if res is not None:
        res.put(resp)
        
    return resp