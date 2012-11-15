from hash_util import *
from socket import *
import time
from threading import Thread
import signal
import sys
import uuid

####################### Structs #######################
class Node():
    ID = 0
    IPAddr = None
    ctrlPort = 7228
    relayPort = 7229

####################### Globals #######################

#Networking
servCtrl = None
servRelay = None
MAX_REC_SIZE = 1024
MAX_CONNECTIONS = 30

#Node
thisNode = Node()
thisNode.ID = hash_str(str(uuid.uuid4()) + str(uuid.uuid4()))
thisNode.IPAddr = ""
thisNode.ctrlPort = 7228
thisNode.relayPort = 7229

prevNode = None

#Finger table
fingerTable = []

############### Signal Handlers and Exit ###############

def exit_signal_handler(signal, frame):
    print "\nClosing Connections."
    graceful_exit(0)
    exit(0)
    
signal.signal(signal.SIGINT, exit_signal_handler)

def graceful_exit(exitCode):
    try:
        servCtrl.shutdown(1)
        servCtrl.close()
    except:
        print "Could not close control port."
    try:
        servRelay.shutdown(1)
        servRelay.close()
    except:
        print "Could not close relay port."
    exit(exitCode)

#################### Network Sockets ####################
    
def handle_ctrl_connection(conn, addr):
    data = conn.recv(MAX_REC_SIZE) 
    if data: 
        conn.send(data)
    print "Closing connection."
    conn.shutdown(1)
    conn.close()
    return

def handle_connection(conn, addr):
    data = conn.recv(MAX_REC_SIZE) 
    if data: 
        conn.send(data)
    print "Closing connection."
    conn.shutdown(1)
    conn.close()
    return

def wait_for_ctrl_connections():
    global servCtrl
    servCtrl = socket(AF_INET, SOCK_STREAM)
    conAddr = (thisNode.IPAddr, thisNode.ctrlPort)
    servCtrl.bind((conAddr))
    servCtrl.listen(MAX_CONNECTIONS) 
    print "Waiting for Control Connections."
    while 1:
        conn, addr = servCtrl.accept() #accept the connection
        print "New Connection."
        t = Thread(target=handle_ctrl_connection, args=(conn, addr))
        t.start()
    return

def wait_for_connections():
    global servRelay
    servRelay = socket(AF_INET, SOCK_STREAM)
    conAddr = (thisNode.IPAddr, thisNode.relayPort)
    servRelay.bind((conAddr))
    servRelay.listen(MAX_CONNECTIONS) 
    print "Waiting for Relay Connections."
    while 1:
        conn, addr = servRelay.accept() #accept the connection
        print "New Connection."
        t = Thread(target=handle_connection, args=(conn, addr))
        t.start()
    return

####################### Routing ########################

def get_next_node(node, key):
    #Not sure if this function will be needed yet
    pass

def get_root_node_request(node, key):
    pass

def get_immediate_successor_node():
    pass

def get_node_predecessor():
    pass

def inform_new_predecessor():
    pass

def inform_new_successor():
    pass

def join_network(existingNode):
    k = generate_lookup_key_with_index(thisNode.ID, 0)

    #send request to existing node to get root node at k
    tmpNode = get_root_node_request(existingNode, k)
    if tmpNode == None:
        return -1

    #inform node and its pred that you are in the middle
    
#################### Misc Functions ####################

def initialise_finger_table():
    global fingerTable
    for i in range(0, KEY_SIZE):
        tmpNode = Node()
        fingerTable.append(tmpNode)
    return


######################### Main #########################
def main():
    print hash_str("test")

    #Start listener threads
    listenCtrlThread = Thread(target=wait_for_ctrl_connections)
    listenCtrlThread.daemon = True
    listenCtrlThread.start()
    listenThread = Thread(target=wait_for_connections)
    listenThread.daemon = True
    listenThread.start()
    
    #Init
    initialise_finger_table()
    join_network(Node()) #Create node from args later

    #Wait forever
    while 1:
        #The threads should never die
        listenCtrlThread.join(1)
        listenThread.join(1)
        
    return 0

if __name__ == "__main__":
    main()
