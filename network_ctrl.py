from message_types import *
from socket import *
import time
from threading import Thread
import pickle

#Networking
MAX_REC_SIZE = 4096
MAX_CONNECTIONS = 30
DEFAULT_TIMEOUT = 10

class CtrlMessage():
    def __init__(self, messageType, data, extra):
        self.messageType = messageType
        self.data = data
        self.extra = extra

def serialize_message(message):
     return pickle.dumps(message)

def unserialize_message(serializedMessage):
    return pickle.loads(serializedMessage)


def wait_for_ctrl_connections(thisNode, handlerFunction):
    global servCtrl
    servCtrl = socket(AF_INET, SOCK_STREAM)
    conAddr = (thisNode.IPAddr, thisNode.ctrlPort)
    servCtrl.bind((conAddr))
    servCtrl.listen(MAX_CONNECTIONS) 
    print "Waiting for Control Connections."
    while 1:
        conn, addr = servCtrl.accept() #accept the connection
        #print "New Connection."
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
        #print "New Connection."
        t = Thread(target=handlerFunction, args=(conn, addr))
        t.start()
    return

def send_ctrl_message_with_ACK(message, messageType, extra, requestNode):
    conn = socket(AF_INET, SOCK_STREAM)
    conn.settimeout(DEFAULT_TIMEOUT * 4)
    nodeAddr = (requestNode.IPAddr, requestNode.ctrlPort)
    conn.connect((nodeAddr))
    conn.send(serialize_message(CtrlMessage(messageType, message, extra)))
    data = conn.recv(MAX_REC_SIZE)
    if data:
        try:
            msg = unserialize_message(data)
        except:
            msg = None
    else:
        msg = None
    conn.shutdown(1)
    conn.close()
    return msg
