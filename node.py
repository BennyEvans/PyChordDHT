from hash_util import *
from network_ctrl import *
from socket import *
import time
from threading import *
import signal
import sys
import uuid
import pickle
import copy

##TODO
# arg parsing

####################### Structs #######################
class Node():
    ID = 0
    IPAddr = "localhost"
    ctrlPort = 7228
    relayPort = 7229

####################### Globals #######################

#Node
thisNode = Node()
thisNode.ID = hash_str(str(uuid.uuid4()) + str(uuid.uuid4()))
thisNode.IPAddr = "localhost"
thisNode.ctrlPort = 7228
thisNode.relayPort = 7229

prevNode = None

#Finger table
fingerTable = []
fingerTableLock = Lock()

############### Signal Handlers and Exit ###############

def exit_signal_handler(signal, frame):
    print "\nClosing Connections."
    graceful_exit(0)
    exit(0)
    
signal.signal(signal.SIGINT, exit_signal_handler)

def graceful_exit(exitCode):
    global servCtrl
    global servRelay
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


################## NETWORK CALLBACKS ###################

def handle_ctrl_connection(conn, addr):
    data = conn.recv(MAX_REC_SIZE)
    conn.settimeout(DEFAULT_TIMEOUT)

    if data:
        message = unserialize_message(data)
        
        if message.messageType == ControlMessageTypes.GET_NEXT_NODE:
            retCode = 0
            tmpNode = find_closest_finger(message.data)
            if tmpNode == thisNode:
                tmpNode = get_immediate_successor_node()
                retCode = 1
            retMsg = CtrlMessage(MessageTypes.MSG_ACK, tmpNode, retCode)
            conn.send(serialize_message(retMsg))
            
        elif message.messageType == ControlMessageTypes.GET_ROOT_NODE_REQUEST:
            tmpNode = get_root_node(message.data)
            retMsg = CtrlMessage(MessageTypes.MSG_ACK, tmpNode, 0)
            conn.send(serialize_message(retMsg))
            
        elif message.messageType == ControlMessageTypes.GET_PREDECESSOR:
            retMsg = CtrlMessage(MessageTypes.MSG_ACK, get_predecessor(), 0)
            conn.send(serialize_message(retMsg))
            
        elif message.messageType == ControlMessageTypes.IS_PREDECESSOR:
            set_predecessor(copy.deepcopy(message.data))
            retMsg = CtrlMessage(MessageTypes.MSG_ACK, 0, 0)
            conn.send(serialize_message(retMsg))
            
        elif message.messageType == ControlMessageTypes.IS_SUCCESSOR:
            set_immediate_successor(copy.deepcopy(message.data))
            retMsg = CtrlMessage(MessageTypes.MSG_ACK, 0, 0)
            conn.send(serialize_message(retMsg))
            
        elif message.messageType == ControlMessageTypes.GET_NEXT_NODE_PREDECESSOR:
            retCode = 0
            tmpNode = find_closest_finger(message.data)
            if tmpNode == thisNode:
                retCode = 1
            retMsg = CtrlMessage(MessageTypes.MSG_ACK, tmpNode, retCode)
            conn.send(serialize_message(retMsg))
            
        elif message.messageType == ControlMessageTypes.UPDATE_FINGER_TABLE:
            update_finger_table(message.data, message.extra)
            conn.send(serialize_message(CtrlMessage(MessageTypes.MSG_ACK, 0, 0)))

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

####################### Routing ########################

#Gets the node closest to key from node, node
def get_next_node(node, key):
    message = send_ctrl_message_with_ACK(key, ControlMessageTypes.GET_NEXT_NODE, 0, node)
    return (message.extra, message.data)

def get_next_node_predecessor(node, key):
    message = send_ctrl_message_with_ACK(key, ControlMessageTypes.GET_NEXT_NODE_PREDECESSOR, 0, node)
    return (message.extra, message.data)


#Gets the root node responsible for key, key
def get_root_node(key):
    global thisNode
    closestNode = find_closest_finger(key)
    
    if closestNode == thisNode:
        #this is the closest node - return the successor
        return get_immediate_successor_node()

    while 1:
        #get next node on the path
        (retCode, tmpNode) = get_next_node(closestNode, key)

        if retCode == 1:
            return tmpNode

        closestNode = tmpNode
        
    return None

#similar to get_root_node only it returns the preceding node
def get_closest_preceding_node(key):
    global thisNode
    closestNode = find_closest_finger(key)
    
    if closestNode == thisNode:
        return thisNode

    while 1:
        #get next node on the path
        (retCode, tmpNode) = get_next_node_predecessor(closestNode, key)

        if retCode == 1:
            return tmpNode

        closestNode = tmpNode
        
    return None
    
def update_finger_table_request(requestNode, updateNode, i):
    data = send_ctrl_message_with_ACK(updateNode, ControlMessageTypes.UPDATE_FINGER_TABLE, i, requestNode)
    return

def update_finger_table(node, i):
    global thisNode
    global fingerTable
    
    fingerTableLock.acquire()
    fingerEntry = fingerTable[i]
    fingerTableLock.release()
    
    if hash_between(node.ID, thisNode.ID, fingerEntry.ID):
        fingerTableLock.acquire()
        fingerTable[i] = copy.deepcopy(node)
        fingerTableLock.release()
        update_finger_table_request(get_predecessor(), node, i)
    return

#Requests to run the get_root_node function
#on requestNode - used to enter the network
def get_root_node_request(requestNode, key):
    message = send_ctrl_message_with_ACK(key, ControlMessageTypes.GET_ROOT_NODE_REQUEST, 0, requestNode)
    return message.data

def get_immediate_successor_node():
    global fingerTable
    fingerTableLock.acquire()
    ret = copy.deepcopy(fingerTable[0])
    fingerTableLock.release()
    return ret

def get_node_predecessor(requestNode):
    message = send_ctrl_message_with_ACK(0, ControlMessageTypes.GET_PREDECESSOR, 0, requestNode)
    return message.data

def inform_new_predecessor(node):
    global thisNode
    data = send_ctrl_message_with_ACK(thisNode, ControlMessageTypes.IS_PREDECESSOR, 0, node)
    return

def inform_new_successor(node):
    global thisNode
    data = send_ctrl_message_with_ACK(thisNode, ControlMessageTypes.IS_SUCCESSOR, 0, node)
    return

def update_others():
    global thisNode
    contactedNodes = []
    for i in range(0, KEY_SIZE):
        searchKey = generate_reverse_lookup_key_with_index(thisNode.ID, i)
        tmpNode = get_closest_preceding_node(searchKey)

        if tmpNode not in contactedNodes:
            contactedNodes.append(tmpNode)
            update_finger_table_request(tmpNode, thisNode, i)

    return
        

def join_network(existingNode):
    global fingerTable
    global thisNode
    
    k = generate_lookup_key_with_index(thisNode.ID, 0)

    #send request to existing node to get root node at k
    tmpNode = get_root_node_request(existingNode, k)
    if tmpNode == None:
        return -1

    fingerTableLock.acquire()
    fingerTable[0] = copy.deepcopy(tmpNode)
    fingerTableLock.release()

    #inform node and its pred that you are now between them
    nextNodesPred = get_node_predecessor(tmpNode)
    inform_new_successor(nextNodesPred)
    inform_new_predecessor(tmpNode)

    for i in range(1, KEY_SIZE):
        searchKey = generate_lookup_key_with_index(thisNode.ID, i)
        
        ##this code below cuts down on the amount of requests needed to generate
	##the table. It checks to see if the previous finger entry is > the lookup entry
        fingerTableLock.acquire()
        prevFingerNode = copy.deepcopy(fingerTable[i-1])
        fingerTableLock.release()
        
        if hash_between(searchKey, thisNode.ID, prevFingerNode.ID):
            fingerTableLock.acquire()
            fingerTable[i] = copy.deepcopy(fingerTable[i-1])
            fingerTableLock.release()
            
        else:
            #Need to make a request
            retNode = get_root_node_request(existingNode, searchKey)
            fingerTableLock.acquire()
            fingerTable[i] = copy.deepcopy(retNode)
            fingerTableLock.release()

    #fingerTableLock.release()

    update_others()
    return 0


    
#################### Misc Functions ####################

## MUTUAL EXCLUSION NEEDS TO BE IMPLEMENTED IN THESE FUNCTIONS

def initialise_finger_table():
    global fingerTable
    fingerTableLock.acquire()
    for i in range(0, KEY_SIZE):
        tmpNode = copy.deepcopy(thisNode)
        fingerTable.append(tmpNode)
    fingerTableLock.release()
    return

def print_finger_table():
    fingerTableLock.acquire()
    for i in range(0, KEY_SIZE):
        print str(fingerTable[i].ID.key) + " " + str(fingerTable[i].IPAddr) + ":" + str(fingerTable[i].ctrlPort)
    fingerTableLock.release()
    return

def find_closest_finger(key):
    global fingerTable
    global thisNode
    fingerTableLock.acquire()
    for i in range((KEY_SIZE - 1), -1, -1):
        if hash_between(fingerTable[i].ID, thisNode.ID, key):
            return copy.deepcopy(fingerTable[i])
    #this must be the closest node
    fingerTableLock.release()
    return thisNode

def set_immediate_successor(node):
    global fingerTable
    fingerTableLock.acquire()
    fingerTable[0] = copy.deepcopy(node)
    fingerTableLock.release()

def set_predecessor(node):
    global prevNode
    prevNode = copy.deepcopy(node)

def get_predecessor():
    global prevNode
    return prevNode

##def serialize_node(node):
##    return pickle.dumps(node)
##
##def unserialize_node(serializedNode):
##    return pickle.loads(serializedNode)
##
##def serialize_key(key):
##    return pickle.dumps(key)
##
##def unserialize_key(serializedKey):
##    return pickle.loads(serializedKey)


######################### Main #########################
def main():
    global thisNode
    print hash_str("test")

    initialise_finger_table()
    set_predecessor(copy.deepcopy(thisNode))

    #Start listener threads
    listenCtrlThread = Thread(target=wait_for_ctrl_connections, args=(thisNode,handle_ctrl_connection))
    listenCtrlThread.daemon = True
    listenCtrlThread.start()
    listenThread = Thread(target=wait_for_connections, args=(thisNode,handle_connection))
    listenThread.daemon = True
    listenThread.start()
    
    #Init
    #if joining do this
    join_network(Node()) #Create node from args later
    #else creator - skip joining and wait for people to connect
    
    print "Joined the network"
    #print_finger_table()
    
    #Wait forever
    while 1:
        #The threads should never die
        listenCtrlThread.join(1)
        listenThread.join(1)
        
    return 0

if __name__ == "__main__":
    main()
