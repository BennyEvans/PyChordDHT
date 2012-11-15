import hashlib

class Key():
    def __init__(self, key):
        self.key = key

#key size
KEY_SIZE = 160

#max chord index
MAX_INDEX = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF

#one index
ONE_INDEX = 0x0000000000000000000000000000000000000001

#returns true if h1>h2
def hash_greater_than(h1, h2):
    if h1.key > h2.key:
        return True
    return False

#returns true if h1<h2
def hash_less_than(h1, h2):
    if h1.key < h2.key:
        return True
    return False

#returns true if h1==h2
def hash_equal(h1, h2):
    if h1.key == h2.key:
        return True
    return False

#returns True if h1 is between s1 and s2
def hash_between(h1, s1, s2):
    
    #if s1 == s2 then h1 must be between them assuming a full loop
    if(hash_equal(s1, s2)):
        return True

    #if h1 == s1 || h1 == s2 then return False
    if(hash_equal(h1, s1) or hash_equal(h1, s2)):
        return False

    #Check if s2 < s1 - if so assume a loop
    if hash_less_than(s2, s1):
        #assume a loop around the circle in which case h1 must be h1 > s1 || h1 < s2
        if hash_greater_than(h1, s1) or hash_less_than(h1, s2):
            return True
    else:
        #normal s1 < h1 < s2
        if hash_greater_than(h1, s1) and hash_less_than(h1, s2):
            return True
        
    return False

def add_keys(k1, k2):
    x = (int(k1.key, 16) + int(k2.key, 16)) % MAX_INDEX
    return Key(hex(x).replace("L", ""))


def subtract_keys(k1, k2):
    x = (int(k1.key, 16) - int(k2.key, 16))
    if x < 0:
        x = MAX_INDEX + x
    return Key(hex(x).replace("L", ""))

    
def hash_str(strToHash):
    m = hashlib.sha1()
    m.update(strToHash)
    return Key("0x" + m.hexdigest())


def generate_key_with_index(index):
    return Key(hex(ONE_INDEX << index).replace("L", ""))


def generate_lookup_key_with_index(thisIndex, indexOfKey):
    return add_keys(thisIndex, generate_key_with_index(indexOfKey))


def generate_reverse_lookup_key_with_index(thisIndex, indexOfKey):
    return subtract_keys(thisIndex, generate_key_with_index(indexOfKey))

##h1 = hash_str("ben0")
##print h1.key
##h2 = hash_str("ben1")
##print h2.key
##h3 = hash_str("ben2")
##print h3.key
##
##print hash_between(h2, h3, h1)
##
##m = add_keys(h1, h2)
##n = subtract_keys(h1, h2)
##print m.key
##print n.key
##print subtract_keys(h3, m).key
##
##print generate_key_with_index(10).key
##
##print generate_lookup_key_with_index(h1, 10).key
##print generate_reverse_lookup_key_with_index(h1,10).key
