import sys, socket
from threading import Thread
import random
from random import randint
import utils, string, time

def get_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    return sock
 
if len(sys.argv) != 2:
    print ("Correct usage: script, partitions")
    exit()
    
partitions = eval(str(sys.argv[1]))

def connect_to_server():
    i = randint(0, len(partitions)-1)
    j = randint(0, len(partitions[i])-1)
    
    ip, port = partitions[i][j].split(':')
    port = int(port)
    
    server = get_socket()
    
    while True:
        try:
            server.connect((ip, port))
            break
        except:
            print("Waiting for server startup....")
    
    return server

request_id = 0
new_server = True

s=string.ascii_lowercase

while True:
    key = ''.join(random.sample(s,random.randint(1, 5)))
    val = random.randint(1, 100000)
    command = f"SET {key} {val} {request_id}" #input()
    print(command)
    
    # command = input()
    # command = command + ' ' + str(request_id)
        
    while True:
        server = connect_to_server()    
        server.send(command.encode())
        
        output = server.recv(2048).decode()
        
        if output == 'ok':
            print(output)
            server.close()
            break
    
    request_id += 1