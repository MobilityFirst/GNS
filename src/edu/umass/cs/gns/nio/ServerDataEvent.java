package edu.umass.cs.gns.nio;
import java.nio.channels.SocketChannel;

class ServerDataEvent {
	public NioServer2 server;
	public SocketChannel socket;
	public byte[] data;
	
	public ServerDataEvent(NioServer2 server, SocketChannel socket, byte[] data) {
		this.server = server;
		this.socket = socket;
		this.data = data;
	}
}