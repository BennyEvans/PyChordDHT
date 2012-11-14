from socket import *
 
HOST = 'localhost'
PORT = 7228
ADDR = (HOST,PORT)
BUFSIZE = 200
 
cli = socket( AF_INET,SOCK_STREAM)
cli.connect((ADDR))
cli = socket( AF_INET,SOCK_STREAM)
cli.connect((ADDR))
cli.send("Its Working!")
data = cli.recv(BUFSIZE)
print data
 
cli.close()
