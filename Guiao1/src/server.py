"""CD Chat server program."""
import logging
import socket
import selectors
import json
from .protocol import CDProto

logging.basicConfig(filename="server.log", level=logging.DEBUG)

class Server:
    """Chat Server process."""

    def __init__(self):
        #                           
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        self.sock.bind(('', 8888))
        self.sock.listen() #
        self.sel = selectors.DefaultSelector() 
        self.sel.register(self.sock, selectors.EVENT_READ, self.accept) 
        self.names = {} 
        self.userchannels = {} 

        

    def accept(self, sock, mask):
        conn, addr = sock.accept()  # Should be ready
        print('accepted', conn, 'from', addr)
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, self.read)
    
    def read(self, conn, mask):
        size= int.from_bytes(conn.recv(2), 'big') 
        data = conn.recv(size).decode("utf-8") # Should be ready
        if data:
            try:
                dataparts = json.loads(data)
            except json.JSONDecodeError:
                return
            command = dataparts["command"]
            if command == "register":
                self.names[conn] = dataparts["user"]
                self.userchannels[conn] = [None] 
                user=dataparts["user"]

            elif command == "message": 
                message = dataparts["message"]
                channel = dataparts["channel"] 
                msg = CDProto.message(message)
                for user in self.userchannels.keys():
                    if channel in self.userchannels[user]:
                        logging.debug(f'{self.names[conn]} in channel {channel} sent: {message}')
                        CDProto.send_msg(user, msg)
                    elif channel == "":
                        logging.debug(f'{self.names[conn]} sent: {message}')
                        CDProto.send_msg(user, msg)

            else:
                channel = dataparts["channel"]
                user_channels = self.userchannels.get(conn)
                if channel in user_channels:
                    print(f"{self.names[conn]} is already in {channel}")
                else:
                    if user_channels != [None]:
                        self.userchannels[conn].append(channel)
                    else:
                        self.userchannels[conn] = [channel]
        else:  
            print('Closing', conn)
            try:
                del self.names[conn]
                del self.userchannels[conn]
            except KeyError:
                pass
            self.sel.unregister(conn)
            conn.close()
    def loop(self):
        """Loop indefinetely."""

        while True:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)