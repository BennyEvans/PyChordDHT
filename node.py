from hash_util import *
from message_types import *
from socket import *
import time
from threading import Thread
import signal
import sys
import uuid
import pickle

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
DEFAULT_TIMEOUT = 10

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
    conn.settimeout(DEFAULT_TIMEOUT)

    if data: 
        if int(data[0]) == ControlMessageTypes.GET_NEXT_NODE:
            tmpNode = find_closest_finger(unserialize_key(data.split(':')[1]))
            if tmpNode == thisNode:
                tmpNode = get_immediate_successor_node()
            conn.send(serialize_node(tmpNode))
        elif int(data[0]) == ControlMessageTypes.GET_ROOT_NODE:
            pass

    print "Closing connection."
    conn.shutdown(1)
    conn.close()
    return

def handle_connection(conn, addr):
    data = conn.recv(MAX_REC_SIZE)
    conn.settimeout(DEFAULT_TIMEOUT)
    if data: 
        conn.send(data)
    print "Closing connection."
    conn.shutdown(1)
    conn.close()
    return

def wait_for_ctrl_connections():
    global servCtrl
    global thisNode
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
    global thisNode
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

def send_ctrl_message_with_ACK(message, messageType, node):
    #timeout
    conn = socket(AF_INET, SOCK_STREAM)
    conn.settimeout(DEFAULT_TIMEOUT)
    nodeAddr = (node.IPAddr, node.ctrlPort)
    conn.connect((nodeAddr))
    conn.send(str(messageType) + ":" + message)
    data = conn.recv(MAX_REC_SIZE)
    conn.shutdown(1)
    conn.close()
    return data

####################### Routing ########################

def get_next_node(node, key):
    serializedNode = send_ctrl_message_with_ACK(ControlMessageTypes.GET_NEXT_NODE, serialize_key(key), node)
    return unserialize_node(serializedNode)


#Gets the root node responsible for key
def get_root_node(key):
    closestNode = find_closest_finger(key)
    
    if closestNode == thisNode:
        #this is the closest node - return the successor
        return get_immediate_successor_node()

    #while 1:
        #get next node on the path
        #get_next_node
    

#Requests to run the get_root_node function
#on requestNode - used to enter the network
def get_root_node_request(requestNode, key):
    pass

def get_immediate_successor_node():
    global fingerTable 
    return fingerTable[0]

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

def find_closest_finger(key):
    global fingerTable
    global thisNode
    for i in range((KEY_SIZE - 1), -1, -1):
        if hash_between(fingerTable[i].ID, thisNode.ID, key):
            return fingerTable[i]
    #this must be the closest node
    return thisNode

def serialize_node(node):
    return pickle.dumps(node)

def unserialize_node(serializedNode):
    return pickle.loads(serializedNode)

def serialize_key(key):
    return pickle.dumps(key)

def unserialize_key(serializedKey):
    return pickle.loads(serializedKey)


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
