"""CD Chat client program"""
import logging
import sys
import socket
import selectors
import os
import fcntl

from .protocol import CDProto, CDProtoBadFormat

logging.basicConfig(filename=f"{sys.argv[0]}.log", level=logging.DEBUG)


class Client:
    """Chat Client process."""

    def __init__(self, name: str = "Foo"):
        """Initializes chat client."""
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.selector = selectors.DefaultSelector()
        self.name = name
        self.channel = ""
        self.channels = []

        # set sys.stdin non_blocking
        orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
        fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)
        self.selector.register(sys.stdin, selectors.EVENT_READ, self.write)


    def connect(self):
        """Connect to chat server and setup stdin flags."""
        self.s.connect(('127.0.0.1', 8888))
        self.selector.register(self.s, selectors.EVENT_READ, self.read)
        message = CDProto.register(self.name)
        CDProto.send_msg(self.s, message)

    def read(self, sock, mask):
        data= CDProto.recv_msg(self.s)
        print(f'<{data.message}')
        logging.debug(f'received: {data.message}')
        
    def write(self, stdin, mask):
        message = stdin.read()
        if message[0:5] == "/join":
            self.channel = message.split(" ")[1]

            if self.channel in self.channels:
                print(f'{self.name} is already in channel "{self.channel}"')
            else:
                self.channels.append(self.channel) # adiciona o channel a lista dos clientes
                message = CDProto.join(self.channel)
                CDProto.send_msg(self.s, message)
        elif message == "exit\n":
            self.selector.unregister(self.s)
            self.s.close()
            sys.exit()
        else:
            message = CDProto.message(message, self.channel)
            CDProto.send_msg(self.s, message)
            logging.debug(f'sent: {message.message}')


    def loop(self):
        """Loop indefinetely."""
        
        while True:
            sys.stdout.write(">")
            sys.stdout.flush()

            for key, mask in self.selector.select():
                callback = key.data
                callback(key.fileobj, mask)
        
