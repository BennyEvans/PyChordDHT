from hash_util import *
from socket import *
import time
from threading import Thread
import signal
import sys

#Networking
nodeCtrlPort = 7228
nodeRelayPort = 7229
nodeAddr = ""
maxConnections = 40
servCtrl = None
servRelay = None

nodeID = ""
fingerTable = [0] * 160

#Signal Handlers
def exit_signal_handler(signal, frame):
    try:
        close(servCtrl)
    except:
        pass
    try:
        close(servRelay)
    except:
        pass 
    sys.exit(0)
    
signal.signal(signal.SIGINT, exit_signal_handler)

def wait_for_ctrl_connections():
    servCtrl = socket(AF_INET, SOCK_STREAM)
    ADDR = (nodeAddr, nodeCtrlPort)
    servCtrl.bind((ADDR))
    servCtrl.listen(maxConnections) 
    print "Now waiting\n"
    sys.stdout.flush()
    while 1:
        conn, addr = servCtrl.accept() #accept the connection
        conn.send('TEST')
        conn.close()

def wait_for_connections():
    servRelay = socket(AF_INET, SOCK_STREAM)
    ADDR = (nodeAddr, nodeRelayPort)
    servRelay.bind((ADDR))
    servRelay.listen(maxConnections) 
    print "Now waiting\n"
    sys.stdout.flush()
    while 1:
        conn,addr = serv.accept() #accept the connection
        conn.send('TEST')
        conn.close()



print hash_str("ben0")



def main():
    print "Here"
    t = Thread(target=wait_for_ctrl_connections)
    t.start()
    t1 = Thread(target=wait_for_connections)
    t1.start()
    t.join()
    t1.join()

if __name__ == "__main__":
    main()
