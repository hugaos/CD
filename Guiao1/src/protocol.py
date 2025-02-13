"""Protocol for chat server - Computação Distribuida Assignment 1."""
import json
from datetime import datetime
from socket import socket
from datetime import datetime

class Message:
    """Message Type."""
    def __init__(self, command):
        self.command = command
    
    def __str__(self):
        return json.dumps({"command": self.command})

class JoinMessage(Message):
    """Message to join a chat channel."""
    def __init__(self, command, channel):
        super().__init__(command)
        self.channel = channel

    def __str__(self):
        return json.dumps({"command": self.command, "channel": self.channel})
class RegisterMessage(Message):
    """Message to register username in the server."""
    def __init__(self, command, username):
        super().__init__(command)
        self.username = username
    def __str__(self):
        return json.dumps({"command": self.command, "user": self.username})
class TextMessage(Message):
    """Message to chat with other clients."""
    def __init__(self, command, message, channel, ts):
        super().__init__(command)
        self.message = message
        self.channel = channel
        self.ts = ts

    def __str__(self):
        data = {"command": self.command, "message": self.message, "ts": self.ts}
        if self.channel is not None:
            data["channel"] = self.channel
        return json.dumps(data)

class CDProto:
    """Computação Distribuida Protocol."""

    @classmethod
    def register(cls, username: str) -> RegisterMessage:
        """Creates a RegisterMessage object."""
        r= RegisterMessage('register', username)
        return r

    @classmethod
    def join(cls, channel: str) -> JoinMessage:
        """Creates a JoinMessage object."""
        j= JoinMessage('join', channel)
        return j

    @classmethod
    def message(cls, message: str, channel: str = None) -> TextMessage:
        """Creates a TextMessage object."""
        ts = int(datetime.now().timestamp()) 
        return TextMessage('message', message, channel, ts)

    @classmethod
    def send_msg(cls, connection: socket, msg: Message):
        """Sends through a connection a Message object."""
        tipo = type(msg)
        if tipo is TextMessage:
            message = { "command": "message", "message": f"{msg.message}", "channel": f"{msg.channel}", "ts": msg.ts }
        if tipo is JoinMessage:
            message = { "command": "join", "channel": f"{msg.channel}" }
        if tipo is RegisterMessage:
            message = { "command": "register", "user": f"{msg.username}" }
        
        message = json.dumps(message).encode("utf-8")
        size = len(message).to_bytes(2, "big")
        connection.send(size + message) 


    @classmethod
    def recv_msg(cls, connection: socket) -> Message:
        """Receives through a connection a Message object."""
        size = int.from_bytes(connection.recv(2), 'big')
        data = connection.recv(size).decode("utf-8")

        try:
            message = json.loads(data)
        except json.JSONDecodeError as err:
            raise CDProtoBadFormat(data)
        
        if message["command"] == "register":
            user = message["user"]
            return CDProto.register(user)
        
        if message["command"]  == "join":
            channel = message["channel"]
            return CDProto.join(channel)

        if message["command"]  == "message":
            msg = message["message"]
            if message.get("channel"):
                return CDProto.message(msg, message["channel"])
            else:
                return CDProto.message(msg)

        raise CDProtoBadFormat()


class CDProtoBadFormat(Exception):
    """Exception when source message is not CDProto."""

    def __init__(self, original_msg: bytes=None) :
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")