from message_types import *
from socket import *
import time
from threading import Thread
import pickle

#Networking
servCtrl = None
servRelay = None
MAX_REC_SIZE = 1024
MAX_CONNECTIONS = 30
DEFAULT_TIMEOUT = 10
ACK = "1"

class CtrlMessage():
    def __init__(self, messageType, data, extra):
        self.messageType = messageType
        self.data = data
        self.extra = extra

def serialize_message(message):
     return pickle.dumps(message)

def unserialize_message(serializedMessage):
    return pickle.loads(serializedMessage)


##def handle_ctrl_connection(conn, addr):
##    data = conn.recv(MAX_REC_SIZE)
##    conn.settimeout(DEFAULT_TIMEOUT)
##
##    if data: 
##        if int(data[0]) == ControlMessageTypes.GET_NEXT_NODE:
##            retCode = "0"
##            tmpNode = find_closest_finger(unserialize_key(data.split(':')[1]))
##            if tmpNode == thisNode:
##                tmpNode = get_immediate_successor_node()
##                retCode = "1"
##            conn.send(retCode + ":" + serialize_node(tmpNode))
##            
##        elif int(data[0]) == ControlMessageTypes.GET_ROOT_NODE_REQUEST:
##            tmpNode = get_root_node(unserialize_key(data.split(':')[1]))
##            conn.send(serialize_node(tmpNode))
##            
##        elif int(data[0]) == ControlMessageTypes.GET_PREDECESSOR:
##            conn.send(serialize_node(get_predecessor()))
##            
##        elif int(data[0]) == ControlMessageTypes.IS_PREDECESSOR:
##            set_predecessor(unserialize_node(data.split(':')[1]))
##            conn.send(ACK)
##            
##        elif int(data[0]) == ControlMessageTypes.IS_SUCCESSOR:
##            set_immediate_successor(unserialize_node(data.split(':')[1]))
##            conn.send(ACK)
##            
##        elif int(data[0]) == ControlMessageTypes.GET_NEXT_NODE_PREDECESSOR:
##            retCode = "0"
##            tmpNode = find_closest_finger(unserialize_key(data.split(':')[1]))
##            if tmpNode == thisNode:
##                retCode = "1"
##            conn.send(retCode + ":" + serialize_node(tmpNode))
##            
##        elif int(data[0]) == ControlMessageTypes.UPDATE_FINGER_TABLE:
##            i = data.split(':')[1]
##            tmpNode = unserialize_node(data.split(':')[2])
##            update_finger_table(tmpNode, i)
##            conn.send(ACK)
##
##    print "Closing connection."
##    conn.shutdown(1)
##    conn.close()
##    return
##
##def handle_connection(conn, addr):
##    data = conn.recv(MAX_REC_SIZE)
##    conn.settimeout(DEFAULT_TIMEOUT)
##    if data: 
##        conn.send(data)
##    print "Closing connection."
##    conn.shutdown(1)
##    conn.close()
##    return

def wait_for_ctrl_connections(thisNode, handlerFunction):
    global servCtrl
    servCtrl = socket(AF_INET, SOCK_STREAM)
    conAddr = (thisNode.IPAddr, thisNode.ctrlPort)
    servCtrl.bind((conAddr))
    servCtrl.listen(MAX_CONNECTIONS) 
    print "Waiting for Control Connections."
    while 1:
        conn, addr = servCtrl.accept() #accept the connection
        print "New Connection."
        t = Thread(target=handlerFunction, args=(conn, addr))
        t.start()
    return

def wait_for_connections(thisNode, handlerFunction):
    global servRelay
    servRelay = socket(AF_INET, SOCK_STREAM)
    conAddr = (thisNode.IPAddr, thisNode.relayPort)
    servRelay.bind((conAddr))
    servRelay.listen(MAX_CONNECTIONS) 
    print "Waiting for Relay Connections."
    while 1:
        conn, addr = servRelay.accept() #accept the connection
        print "New Connection."
        t = Thread(target=handlerFunction, args=(conn, addr))
        t.start()
    return

def send_ctrl_message_with_ACK(message, messageType, node):
    conn = socket(AF_INET, SOCK_STREAM)
    conn.settimeout(DEFAULT_TIMEOUT * 4)
    nodeAddr = (node.IPAddr, node.ctrlPort)
    conn.connect((nodeAddr))
    conn.send(str(messageType) + ":" + message)
    data = conn.recv(MAX_REC_SIZE)
    conn.shutdown(1)
    conn.close()
    return data
