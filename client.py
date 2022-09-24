import sys
from threading import Thread
from socket_handler import get_socket
 
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

while True:
    command = input()
    server.send(command.encode())