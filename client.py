import sys, socket
from threading import Thread

def get_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    return sock
 
if len(sys.argv) != 3:
    print ("Correct usage: script, IP address, port number")
    exit()

server_ip_address = str(sys.argv[1])
server_port = int(sys.argv[2])

server = get_socket()
server.connect((server_ip_address, server_port))

def listen_for_messages():
    while True:
        output = server.recv(2048).decode()
        print(output)

t = Thread(target=listen_for_messages)
t.daemon = True
t.start()

request_id = 0

while True:
    command = input()
    command = command + ' ' + str(request_id)
    server.send(command.encode())
    request_id += 1