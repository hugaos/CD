import socket
import threading
import logging
import time
import pickle
from utils import dht_hash, contains

class FingerTable:
    """Finger Table."""

    def __init__(self, node_id, node_addr, m_bits=10):
        self.m_bits = m_bits
        self.node_id = node_id
        self.node_addr = node_addr
        self.finger_table = [(node_id, node_addr) for _ in range(m_bits)]

    def fill(self, node_id, node_addr):
        self.finger_table = [(node_id, node_addr) for _ in range(self.m_bits)]

    def update(self, index, node_id, node_addr):
        self.finger_table[index-1] = (node_id, node_addr)

    def find(self, identification):
        for i in range(self.m_bits):
            if contains(self.node_id, self.finger_table[i][0], identification):
                return self.finger_table[i-1][1]
        return self.finger_table[-1][1]

    def refresh(self):
        return [(i + 1, (self.node_id + 2 ** i) % (2 ** self.m_bits), self.finger_table[i][1]) for i in range(self.m_bits)]

    def getIdxFromId(self, id):
        for i in range(self.m_bits):
            value = (self.node_id + 2 ** i) % (2 ** self.m_bits)
            if contains(self.node_id, value, id):
                return i + 1

    def __repr__(self):
        return str(self.finger_table)

    @property
    def as_list(self):
        return self.finger_table


class DHTNode(threading.Thread):
    """ DHT Node Agent. """

    def __init__(self, address, dht_address=None, timeout=3):
        threading.Thread.__init__(self)
        self.done = False
        self.identification = dht_hash(address.__str__())
        self.addr = address  
        self.dht_address = dht_address  
        self.stored_timestamp=None;
        if dht_address is None:
            self.inside_dht = True
         
            self.successor_id = self.identification
            self.successor_addr = address
            self.predecessor_id = None
            self.predecessor_addr = None
        else:
            self.inside_dht = False
            self.successor_id = None
            self.successor_addr = None
            self.predecessor_id = None
            self.predecessor_addr = None

        self.finger_table = FingerTable(
            self.identification, self.addr, m_bits=10)  

        self.keystore = {}  
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(timeout)
        self.logger = logging.getLogger("Node {}".format(self.identification))
    def send(self, address, msg):
        payload = pickle.dumps(msg)
        self.socket.sendto(payload, address)

    def recv(self):
        try:
            payload, addr = self.socket.recvfrom(1024)
        except socket.timeout:
            return None, None
        if len(payload) == 0:
            return None, addr
        return payload, addr

    def node_join(self, args):
        self.logger.debug("Node join: %s", args)
        addr = args["addr"]
        identification = args["id"]
        if self.identification == self.successor_id:
            self.successor_id = identification
            self.successor_addr = addr
            self.finger_table.fill(identification, addr)
            args = {"successor_id": self.identification, "successor_addr": self.addr}
            self.send(addr, {"method": "JOIN_REP", "args": args})
        elif contains(self.identification, self.successor_id, identification):
            args = {"successor_id": self.successor_id, "successor_addr": self.successor_addr}
            self.successor_id = identification
            self.successor_addr = addr
            self.finger_table.fill(identification, addr)
            self.send(addr, {"method": "JOIN_REP", "args": args})
        else:
            self.logger.debug("Find Successor(%d)", args["id"])
            self.send(self.successor_addr, {"method": "JOIN_REQ", "args": args})
        self.logger.info(self)

    
    def node_leave(self, args):
        leaving_id = args["id"]
        leaving_addr = args["addr"]
        
        # Update finger table
        self.update_finger_table_on_leave(leaving_id, leaving_addr)
        
        # Update successor and predecessor
        if self.successor_id == leaving_id:
            self.successor_id = args["successor_id"]
            self.successor_addr = args["successor_addr"]
            self.finger_table.fill(self.successor_id, self.successor_addr)
        if self.predecessor_id == leaving_id:
            self.predecessor_id = None
            self.predecessor_addr = None
            
    def update_finger_table_on_leave(self, leaving_id, leaving_addr):
        for i in range(self.finger_table.m_bits):
            start = (self.identification + 2 ** i) % (2 ** self.finger_table.m_bits)
            if contains(start, self.finger_table.finger_table[i][0], leaving_id):
                self.finger_table.update(i + 1, self.successor_id, self.successor_addr)

    def get_successor(self, args):
        self.logger.debug("Get successor: %s", args)
        if contains(self.identification, self.successor_id, args["id"]):
            self.send(args["from"], {"method": "SUCCESSOR_REP", "args": {
                "req_id": args["id"], "successor_id": self.successor_id, "successor_addr": self.successor_addr}})
        else:
            self.send(self.successor_addr, {"method": "SUCCESSOR", 'args': {
                "id": args["id"], "from": args["from"]}})

    def notify(self, args):
        self.logger.debug("Notify: %s", args)
        if self.predecessor_id is None or contains(self.predecessor_id, self.identification, args["predecessor_id"]):
            self.predecessor_id = args["predecessor_id"]
            self.predecessor_addr = args["predecessor_addr"]
        self.logger.info(self)

    def stabilize(self, from_id, addr):
        self.logger.debug("Stabilize: %s %s", from_id, addr)
        if from_id is not None and contains(self.identification, self.successor_id, from_id):
            self.successor_id = from_id
            self.successor_addr = addr

        args = {"predecessor_id": self.identification, "predecessor_addr": self.addr}
        self.send(self.successor_addr, {"method": "NOTIFY", "args": args})

        for _, next_id, _ in self.finger_table.refresh():
            self.get_successor({"id": next_id, "from": self.addr})

    def put(self, key, value, address):
        key_hash = dht_hash(key)
        self.logger.debug("Put: %s %s", key, key_hash)

        if contains(self.identification, self.successor_id, key_hash):
            self.send(self.successor_addr, {"method": "PUT", "args": {"key": key, "value": value, "from": address}})
        elif contains(self.predecessor_id, self.identification, key_hash):
            if key not in self.keystore:
                self.keystore[key] = value
                self.send(address, {"method": "ACK"})
            else:
                self.send(address, {"method": "NACK"})
        else:
            self.send(self.finger_table.find(key_hash), {"method": "PUT", "args": {"key": key, "value": value, "from": address}})

    def get(self, key, address):
        key_hash = dht_hash(key)
        self.logger.debug("Get: %s %s", key, key_hash)

        if contains(self.identification, self.successor_id, key_hash):
            self.send(self.successor_addr, {"method": "GET", "args": {"key": key, "from": address}})
        elif contains(self.predecessor_id, self.identification, key_hash):
            if key in self.keystore:
                value = self.keystore[key]
                self.send(address, {"method": "ACK", "args": value})
            else:
                self.send(address, {"method": "NACK"})
        else:
            self.send(self.finger_table.find(key_hash), {"method": "GET", "args": {"key": key, "from": address}})

    def run(self):
        self.socket.bind(self.addr)
        while not self.inside_dht:
            join_msg = {"method": "JOIN_REQ", "args": {"addr": self.addr, "id": self.identification}}
            self.send(self.dht_address, join_msg)
            payload, addr = self.recv()
            if payload is not None:
                output = pickle.loads(payload)
                self.logger.debug("O: %s", output)
                if output["method"] == "JOIN_REP":
                    args = output["args"]
                    self.successor_id = args["successor_id"]
                    self.successor_addr = args["successor_addr"]
                    self.finger_table.fill(self.successor_id, self.successor_addr)
                    self.inside_dht = True
                    self.logger.info(self)
        
        while not self.done:
            payload, addr = self.recv()
            if payload is not None:
                output = pickle.loads(payload)
                self.logger.info("O: %s", output)
                if output["method"] == "JOIN_REQ":
                    self.node_join(output["args"])
                elif output["method"] == "LEAVE_REQ":
                    self.node_leave(output["args"])
                elif output["method"] == "NOTIFY":
                    self.notify(output["args"])
                elif output["method"] == "PUT":
                    self.put(output["args"]["key"], output["args"]["value"], output["args"].get("from", addr))
                elif output["method"] == "GET":
                    self.get(output["args"]["key"], output["args"].get("from", addr))
                elif output["method"] == "PREDECESSOR":
                    self.send(addr, {"method": "STABILIZE", "args": self.predecessor_id})
                elif output["method"] == "SUCCESSOR":
                    self.get_successor(output["args"])
                elif output["method"] == "STABILIZE":
                    self.stabilize(output["args"], addr)    
                elif output["method"] == "SUCCESSOR_REP":
                    index = output["args"]["req_id"]
                    succ_id = output["args"]["successor_id"]
                    succ_addr = output["args"]["successor_addr"]
                    self.finger_table.update(self.finger_table.getIdxFromId(index), succ_id, succ_addr)
            else:
                if  self.stored_timestamp==None:
                    self.send(self.successor_addr, {"method": "PREDECESSOR"})
                    self.stored_timestamp=time.time() 
                elif time.time()-self.stored_timestamp>5:
                    sid=self.successor_id
                    saddr=self.successor_addr
                    sssid=self.finger_table.finger_table[0][0]
                    ssaddr=self.finger_table.finger_table[0][1]
                    self.node_join({"addr":ssaddr,"id":sssid})



    def __str__(self):
        return "Node ID: {}; DHT: {}; Successor: {}; Predecessor: {}; FingerTable: {}".format(
            self.identification,
            self.inside_dht,
            self.successor_id,
            self.predecessor_id,
            self.finger_table,
        )

    def __repr__(self):
        return self.__str__()
