from enum import Enum
from typing import Any, Callable
import json
import pickle
import socket
import xml.etree.ElementTree as XML


class MiddlewareType(Enum):
    """Middleware Type."""
    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.type = _type
        self.topic = topic
        self._host = "localhost"
        self._port = 5000
        self._addr = (self._host, self._port)

        self.middleware = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.middleware.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.middleware.connect(self._addr)

    def push(self, value):
        """Sends data to broker."""
        size = len(value).to_bytes(2, "big")
        self.middleware.send(size + value)

    def pull(self) -> (str, Any):
        """Receives (topic, data) from broker.

        Should BLOCK the consumer!"""
        messageSize = int.from_bytes(self.middleware.recv(2), 'big')

        if messageSize != 0:
            message = self.middleware.recv(messageSize)
            return message, None

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        message = {"command": "list_topics"}
        self.middleware.send(len(message).to_bytes(2, "big") + message)

    def cancel(self):
        """Cancel subscription."""
        message = {"command": "cancel", "topic": self.topic}
        self.middleware.send(len(message).to_bytes(2, "big") + message)


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        message = pickle.dumps({"command": "ACK", "format": "JSON"})
        self.middleware.send(len(message).to_bytes(2, "big") + message)

        if _type == MiddlewareType.CONSUMER:
            message = json.dumps({"command": "subscribe", "topic": topic})
            size = len(message).to_bytes(2, "big")
            encoded_message = message.encode('utf-8')

            self.middleware.send(size + encoded_message)

    def push(self, value):
        message = json.dumps(
            {"command": "publish", "topic": self.topic, "message": value})
        messageEncoded = message.encode('utf-8')
        super().push(messageEncoded)

    def pull(self) -> (str, Any):
        messageSize = int.from_bytes(self.middleware.recv(2), 'big')

        if messageSize != 0:
            message = self.middleware.recv(messageSize)
            return json.loads(message)["topic"], json.loads(message)["message"]


class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        message = pickle.dumps({"command": "ACK", "format": "XML"})
        self.middleware.send(len(message).to_bytes(2, "big") + message)

        if _type == MiddlewareType.CONSUMER:
            root = XML.Element('root')
            XML.SubElement(root, 'command').set("value", "subscribe")
            XML.SubElement(root, 'topic').set("value", str(topic))
            messageEncoded = XML.tostring(root)

            super().push(messageEncoded)

    def push(self, value):
        root = XML.Element('root')
        XML.SubElement(root, 'command').set("value", "publish")
        XML.SubElement(root, 'topic').set("value", self.topic)
        XML.SubElement(root, 'message').set("value", str(value))
        messageEncoded = XML.tostring(root)

        super().push(messageEncoded)

    def pull(self) -> (str, Any):
        messageSize = int.from_bytes(self.middleware.recv(2), "big")

        if messageSize != 0:
            message = self.middleware.recv(messageSize)

            decoded_message = {}
            for branch in XML.fromstring(message):
                decoded_message[branch.tag] = branch.attrib["value"]

            return decoded_message["topic"], decoded_message["message"]


class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        message = pickle.dumps({"command": "ACK", "format": "PICKLE"})
        self.middleware.send(len(message).to_bytes(2, "big") + message)

        if _type == MiddlewareType.CONSUMER:
            message = pickle.dumps({"command": "subscribe", "topic": topic})
            size = len(message).to_bytes(2, "big")

            self.middleware.send(size + message)

    def push(self, value):
        message = pickle.dumps(
            {"command": "publish", "topic": self.topic, "message": value})
        size = len(message).to_bytes(2, "big")
        self.middleware.send(size + message)

    def pull(self) -> (str, Any):
        messageSize = int.from_bytes(self.middleware.recv(2), "big")

        if messageSize != 0:
            message = self.middleware.recv(messageSize)
            return pickle.loads(message)["topic"], pickle.loads(message)["message"]
