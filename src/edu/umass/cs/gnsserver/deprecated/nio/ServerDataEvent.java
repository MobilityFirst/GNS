package edu.umass.cs.gnsserver.deprecated.nio;

import java.nio.channels.SocketChannel;

import edu.umass.cs.nio.NIOTransport;

/* This class is deprecated. It is not substantively used and will be removed. 
 */
@Deprecated
@SuppressWarnings("unchecked")
class ServerDataEvent {

  public NIOTransport server;
  public SocketChannel socket;
  public byte[] data;

  public ServerDataEvent(NIOTransport server, SocketChannel socket, byte[] data) {
    this.server = server;
    this.socket = socket;
    this.data = data;
  }
}
