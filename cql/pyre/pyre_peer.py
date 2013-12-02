import time
import zmq

class PyrePeer(object):
    
    PEER_EXPIRED = 10            # expire after 10s
    PEER_EVASIVE = 5             # mark evasive after 5s

    def __init__(self, ctx, identity):
        # TODO: what to do with container?
        self._ctx = ctx          # ZMQ context
        self.mailbox = None      # Socket through to peer
        self.identity = identity # Identity UUID
        self.endpoint = None     # Endpoint connected to
        self.evasive_at = 0      # Peer is being evasive
        self.expired_at = 0      # Peer has expired by now
        self.connected = False   # Peer will send messages
        self.ready = False       # Peer has said Hello to us
        self.status = 0         # Our status counter
        self.sent_sequence = 0   # Outgoing message sequence
        self.want_sequence = 0   # Incoming message sequence
        self.headers = []        # Peer headers

    #def __del__(self):

    # Connect peer mailbox
    def connect(self, reply_to, endpoint):
        if self.connected:
            return
        
        # Create new outgoing socket (drop any messages in transit)
        self.mailbox = zmq.Socket(self._ctx, zmq.DEALER)
        # Set our caller 'From' identity so that receiving node knows
        # who each message came from.
        self.mailbox.setsockopt(zmq.IDENTITY, reply_to.bytes)
        # Set a high-water mark that allows for reasonable activity
        self.mailbox.setsockopt(zmq.SNDHWM, PyrePeer.PEER_EXPIRED * 100)
        # Send messages immediately or return EAGAIN
        self.mailbox.setsockopt(zmq.SNDTIMEO, 0)
        # Connect through to peer node
        #print("tcp://%s" %endpoint)
        self.mailbox.connect("tcp://%s" %endpoint)
        self.endpoint = endpoint
        self.connected = True
        self.ready = False
        
    # Disconnect peer mailbox
    # No more messages will be sent to peer until connected again
    def disconnect(self):
        # If connected, destroy socket and drop all pending messages
        if (self.connected):
            self.mailbox.close()
            self.mailbox = None
            self.endpoint = None
            self.connected = False

    # Send message to peer
    def send(self, msg):
        if self.connected:
            self.sent_sequence += 1
            msg.set_sequence(self.sent_sequence)
            #try:
            msg.send(self.mailbox)
            print("PyrePeer send %s" %msg.struct_data)
            #except Exception as e:
            #    print("msg send failed, %s" %e)
            #    self.disconnect()
            #zre_msg_set_sequence (*msg_p, ++(self->sent_sequence));
            #if (zre_msg_send (msg_p, self->mailbox) && errno == EAGAIN) {
                #self.disconnect()
                #return -1;
        else:
            print("Peer %s not connected" % peer)

    # Return peer connected status
    def is_connected(self):
        return self.connected

    # Return peer identity string
    def get_identity(self):
        return self.identity

    # Return peer connection endpoint
    def get_endpoint(self):
        if self.connected:
            return self.endpoint
        else:
            return ""

    # Register activity at peer
    def refresh(self):
        self.evasive_at = time.time() + self.PEER_EVASIVE
        self.expired_at = time.time() + self.PEER_EXPIRED

    # Return future evasive time
    def evasiv_at(self):
        return self.evasive_at

    # Return future expired time
    def expired_at(self):
        return self.expired_at

    # Return peer status
    def get_status(self):
        return self.status

    # Set peer status
    def set_status(self, status):
        self.status = status

    # Return peer ready state
    def get_ready(self):
        return self.ready

    # Set peer ready state
    def set_ready(self, ready):
        self.ready = ready

    # Get peer header value
    def get_header(self, key):
        return self.headers.get(key, None)

    # Set peer headers
    def set_headers(self, headers):
        self.headers = headers

    # Check peer message sequence
    def check_message(self, msg):
        recd_sequence = msg.get_sequence()
        self.want_sequence += 1
        if self.want_sequence == recd_sequence:
            return True
        else:
            self.want_sequence -= 1
            return False
        