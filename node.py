from hash_util import *
from socket import *
import time
from threading import Thread
import signal
import sys

####################### Globals #######################

#Networking
nodeCtrlPort = 7228
nodeRelayPort = 7229
nodeAddr = ""
maxConnections = 40
servCtrl = None
servRelay = None
MAX_REC_SIZE = 1024

#Node
nodeID = ""
fingerTable = [0] * 160

############### Signal Handlers and Exit ###############

def exit_signal_handler(signal, frame):
    print "Closing Connections"
    graceful_exit(0)
    
signal.signal(signal.SIGINT, exit_signal_handler)

def graceful_exit(exitCode):
    try:
        servCtrl.close()
    except:
        pass
    try:
        servRelay.close()
    except:
        pass
    exit(exitCode)

#################### Network Sockets ####################
    
def handle_ctrl_connection(conn, addr):
    data = conn.recv(MAX_REC_SIZE) 
    if data: 
        conn.send(data) 
    conn.close()

def handle_connection(conn, addr):
    data = conn.recv(MAX_REC_SIZE) 
    if data: 
        conn.send(data) 
    conn.close()

def wait_for_ctrl_connections():
    global servCtrl
    servCtrl = socket(AF_INET, SOCK_STREAM)
    ADDR = (nodeAddr, nodeCtrlPort)
    servCtrl.bind((ADDR))
    servCtrl.listen(maxConnections) 
    print "Waiting for Control Connections."
    while 1:
        conn, addr = servCtrl.accept() #accept the connection
        t = Thread(target=handle_ctrl_connection, args=(conn, addr))
        t.start()

def wait_for_connections():
    global servRelay
    servRelay = socket(AF_INET, SOCK_STREAM)
    ADDR = (nodeAddr, nodeRelayPort)
    servRelay.bind((ADDR))
    servRelay.listen(maxConnections) 
    print "Waiting for Relay Connections."
    while 1:
        conn, addr = servRelay.accept() #accept the connection
        t = Thread(target=handle_connection, args=(conn, addr))
        t.start()


######################### Main #########################
def main():
    print hash_str("test")
    try:
        listenCtrlThread = Thread(target=wait_for_ctrl_connections)
        listenCtrlThread.daemon = True
        listenCtrlThread.start()
        listenThread = Thread(target=wait_for_connections)
        listenThread.daemon = True
        listenThread.start()
        while 1:
            #The threads should never die
            listenCtrlThread.join(1)
            listenThread.join(1)
    except (KeyboardInterrupt, SystemExit):
        graceful_exit(0)

if __name__ == "__main__":
    main()
