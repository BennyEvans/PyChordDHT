from socket import *
 
HOST = 'localhost'
PORT = 7228
ADDR = (HOST,PORT)
ADDR1 = (HOST,7229)
BUFSIZE = 200
 
cli = socket(AF_INET, SOCK_STREAM)
cli.connect((ADDR))

cli1 = socket(AF_INET, SOCK_STREAM)
cli1.connect((ADDR))

cli3 = socket(AF_INET, SOCK_STREAM)
cli3.connect((ADDR1))

cli.send("Its Working - 1!")
data = cli.recv(BUFSIZE)
print data

cli1.send("Its Working - 2!")
data1 = cli1.recv(BUFSIZE)
print data1

cli3.send("Its Working - 3!")
data2 = cli3.recv(BUFSIZE)
print data2
 
cli.close()
cli1.close()
cli3.close()
