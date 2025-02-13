"""Message Broker"""
import enum
from queue import Empty
from typing import Dict, List, Any, Tuple
import socket
import selectors
import json
import pickle
import xml.etree.ElementTree as XMLTree


class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2


class Broker:
    """Implementation of a PubSub Message Broker."""

    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self._host = "localhost"
        self._port = 5000
        print("Listen", self._host, self._port)

        self.broker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.selector = selectors.DefaultSelector()

        self.broker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.broker.bind((self._host, self._port))
        self.broker.listen(20)

        self.selector.register(self.broker, selectors.EVENT_READ, self.accept)

        self.topic_messages = {}
        self.topic_Consumers = {}
        self.consumers_Format = {}

    def accept(self, broker_socket, mask):
        """Accepts a new connection"""
        conn, address = broker_socket.accept()
        """print('Accepted connection from {}:{}'.format(address[0], address[1]))"""

        self.selector.register(conn, selectors.EVENT_READ, self.handle_data)

    def handle_data(self, conn, mask):
        size = int.from_bytes(conn.recv(2), "big")
        if size != 0:
            msg = conn.recv(size)
            serializer = self.consumers_Format.get(conn, Serializer.PICKLE)
            if serializer == Serializer.JSON:
                dic_msg = json.loads(msg)
            elif serializer == Serializer.PICKLE:
                dic_msg = pickle.loads(msg)
            elif serializer == Serializer.XML:
                tree = XMLTree.fromstring(msg)

                dic_msg = {}
                for child in tree:
                    dic_msg[child.tag] = child.attrib["value"]
            else:
                return None

            if not dic_msg:
                print(">> one socket has left")
                conn.close()
                return

            method = dic_msg["command"]
            method_switch = {
                "ACK": self.handle_ack,
                "subscribe": self.handle_subscribe,
                "publish": self.handle_publish,
                "list_topics": self.handle_list_topics,
                "cancel": self.handle_cancel,
            }

            if method in method_switch:
                method_switch[method](conn, dic_msg)
            else:
                print(f">> unknown method '{method}'")
        else:
            self.handle_disconnect(conn)
            return

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""
        topics = [topic for topic, value in self.topic_messages.items() if value != ""]
        return topics


    def get_topic(self, topic: str):
        """Returns the currently stored value in topic."""
        return self.topic_messages.get(topic)

    def put_topic(self, topic: str, value):
        """Store in topic the value."""
        self.topic_Consumers.setdefault(topic, [])
        self.topic_messages[topic] = value

    def list_subscriptions(self, topic: str) -> List[Tuple[socket.socket, Serializer]]:
        """Provide list of subscribers to a given topic."""
        return self.topic_Consumers.get(topic, [])

    def handle_disconnect(self, conn):
        """Handles disconnection of client."""
        for topic in self.topic_Consumers:
            self.unsubscribe(topic, conn)
        self.selector.unregister(conn)
        conn.close()

    def handle_ack(self, conn, dic_msg):
        """Handles ACK command."""
        _format = dic_msg["format"]
        self.consumers_Format[conn] = Serializer[_format.upper()]
        print(f">> nova socket: {_format.lower()}")

    def handle_subscribe(self, conn, dic_msg):
        """Handles subscribe command."""
        topic = dic_msg["topic"]
        _format = self.consumers_Format[conn]
        self.subscribe(topic, conn, _format)
        print(">> subscrição no tópico", topic)

    def handle_publish(self, conn, dic_msg):
        """Handles publish command."""
        topic = dic_msg["topic"]
        msg = dic_msg["message"]
        self.topic_messages[topic] = msg
        print(">> publicação no tópico", topic, "com mensagem", msg)
        for t in list(self.topic_Consumers.keys()):
            if t in topic:
                for conn in self.topic_Consumers[t]:
                    _format = conn[1]
                    _sock = conn[0]
                    dic = {"command": "publish", "topic": topic, "message": msg}
                    self.send(_sock, dic, _format)

    def handle_list_topics(self, conn, dic_msg):
        """Handles list_topics command."""
        lst = self.list_topics()
        dic = {"command": "list_topics", "topic": None, "message": lst}
        self.send(conn, dic, self.consumers_Format[conn])

    def handle_cancel(self, conn, dic_msg):
        """Handles cancel command."""
        topic = dic_msg["topic"]
        self.topic_messages[topic].remove(conn)
        print(f">> subscrição cancelada do topico {topic}")

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        if (address, _format) not in self.consumers_Format:
            self.consumers_Format[(address, _format)] = _format

        if topic not in self.topic_Consumers:
            self.topic_Consumers[topic] = []
        self.topic_Consumers[topic].append((address, _format))

        if topic in self.topic_messages:
            message = {"command": "subscribe", "topic": topic, "message": self.topic_messages[topic]}
            self.send(address, message, _format)

    def send(self, conn, msg, format):
        """Sends message to the connection."""
        if format == Serializer.JSON:
            encoded_msg = json.dumps(msg).encode("utf-8")
        elif format == Serializer.PICKLE:
            encoded_msg = pickle.dumps(msg)
        elif format == Serializer.XML:
            root = XMLTree.Element('root')
            for key in msg.keys():
                XMLTree.SubElement(root, str(key)).set("value", str(msg[key]))
            encoded_msg = XMLTree.tostring(root)
        size = len(encoded_msg).to_bytes(2, "big")
        conn.send(size + encoded_msg)

    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        for consumer in self.topic_Consumers[topic]:
            if consumer[0] == address:
                self.topic_Consumers[topic].remove(consumer)

    def run(self):
        """Run until canceled."""
        while not self.canceled:
            events = self.selector.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
