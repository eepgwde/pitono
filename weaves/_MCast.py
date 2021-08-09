# @file POSet.py
# @brief Partially-ordered sets.
# @author weaves
#
# @details
# This class provides some generators for partially-ordered sets.
#
# @note:
# 

import logging
import socket
import struct
import asyncio
import queue
import threading

# logging.basicConfig(filename='POSet.log', level=logging.DEBUG)
logger = logging.getLogger('MCast')
# sh = logging.StreamHandler()
# logger.addHandler(sh)


## Helper methods
# These methods are the ones being ported to Cython.

## End Helper Methods

class Enqueue(asyncio.DatagramProtocol):

  q0 = None

  def __init__(self, loop, q0):
    self.loop = loop
    self.transport = None
    self.q0 = q0

  def connection_made(self, transport):
    self.transport = transport

  def datagram_received(self, data, addr):
    print('Received  {!r} from {!r}'.format(data, addr))
    data = "I received {!r}".format(data).encode("ascii")
    self.q0.put_nowait(data)

class StoppableThread(threading.Thread):
  """Thread class with a stop() method. The thread itself has to check
  regularly for the stopped() condition."""

  def __init__(self, *args, **kwargs):
    super(StoppableThread, self).__init__(*args, **kwargs)
    self._stop_event = threading.Event()

  def stop(self):
    self._stop_event.set()

  def abort(self):
    """
    If we have issued a stop on this thread we call the undocumented _stop() method.
    """
    if self.stopped():
      try:
        self._stop()
      except:
        pass

  def stopped(self):
    return self._stop_event.is_set()

class Impl(object):
  """
  Multicast support class.

  This is accessed via a Singleton known as MCast
  """

  _logger = logging.getLogger('weaves')
  """Interface with logging"""

  def __init__(self, **kwargs):
    pass

  socks = []
  threads = []

  def make(self, **kwargs):
    """
    Generic factory for this class.

    socket="mcast"
    optional and defaults: broadcast="239.255.255.250" port=1910
    Returns a multicast IPv4 socket for port 1910.

    thread="mcast" 
    options and defaults: listener0=socket queue0=queue daemon=False debug=False
    Returns a Stoppable thread that supports a stop() method and the queue.
    The listener0 is the default socket. The queue0 is a Queue.

    """
    if "socket" in kwargs and kwargs["socket"].startswith("mcast"):
      broadcast=kwargs.get("broadcast", "239.255.255.250")
      port=kwargs.get("port", 1910)
      self._logger.info("socket: {broadcast} {port}".format(broadcast=broadcast, port=port))

      addrinfo = socket.getaddrinfo(broadcast, None)[0]
      sock = socket.socket(addrinfo[0], socket.SOCK_DGRAM)
      sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      ## An error
      # sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

      group_bin = socket.inet_pton(addrinfo[0], addrinfo[4][0])
      if addrinfo[0] == socket.AF_INET: # IPv4
        sock.bind(('', port))
        mreq = group_bin + struct.pack('=I', socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
      else:
        sock.bind(('', port))
        mreq = group_bin + struct.pack('@I', 0)
        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_JOIN_GROUP, mreq)

      self.socks.append(sock)
      return sock

    if "thread" in kwargs and kwargs["thread"].startswith("mcast"):
      listener0 = kwargs.get("listener0", self.make(socket="mcast"))
      queue0 = kwargs.get("queue0", queue.Queue())

      loop = asyncio.new_event_loop()
      loop.set_debug(kwargs.get("debug", False))

      listen = loop.create_datagram_endpoint(
          lambda: Enqueue(loop, queue0),
          sock=listener0,
      )
      transport, protocol = loop.run_until_complete(listen)

      ## loop.run_forever()

      def start_loop(loop):
          asyncio.set_event_loop(loop)
          loop.run_forever()

      t = StoppableThread(target=start_loop, args=(loop,))
      t.daemon = kwargs.get("daemon", False)
      self.threads.append(t)
      return (t, queue0)


    def __del__(self):
      """
      Close any remaining resources.
      """

      if socks is not None:
        if isinstance(socks, list):
          for sock in socks:
            try:
              sock.shutdown()
              sock.close()
            except:
              continue

      if threads is not None:
        if isinstance(threads, list):
          for thr in threads:
            try:
              thr.stop()
              thr.join(timeout=0.5)
              thr._stop()
            except:
              continue


class Singleton(object):
  """
  Single instance of L{Impl} this provides access to the implementation.
  """
  _impl = None
  
  @classmethod
  def instance(cls):
    if cls._impl is None:
      cls._impl =  Impl()

    return cls._impl
